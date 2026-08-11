import express from "express";
import cors from "cors";
import fs from "fs";
import path from "path";
import { randomUUID } from "crypto";
import { backup, DatabaseSync } from "node:sqlite";
import {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
} from "@whiskeysockets/baileys";
import pino from "pino";
import QRCode from "qrcode";
import { extractLidPhoneMapping, resolveMessageJid, resolveSenderJid } from "./jid.js";

// -----------------------------------------------------------------------------
// Configuración
// -----------------------------------------------------------------------------

const PORT = Number(process.env.PORT || 3000);
const API_TOKEN = (process.env.API_TOKEN || "").trim();
const DATABASE_PATH = path.resolve(process.env.DATABASE_PATH || "./data/whatsapp.sqlite");
const BACKUP_DIR = path.resolve(
  process.env.BACKUP_DIR || path.join(path.dirname(DATABASE_PATH), "backups"),
);
const BACKUP_INTERVAL_HOURS = readPositiveInteger(process.env.BACKUP_INTERVAL_HOURS, 24);
const BACKUP_RETENTION_COUNT = readPositiveInteger(process.env.BACKUP_RETENTION_COUNT, 3);

// Si se configura, cada mensaje entrante se reenvía al proyecto web.
// El bot sigue funcionando aunque no exista webhook todavía.
const INCOMING_WEBHOOK_URL = (process.env.INCOMING_WEBHOOK_URL || "").trim();
const INCOMING_WEBHOOK_TOKEN = (process.env.INCOMING_WEBHOOK_TOKEN || "").trim();

if (!API_TOKEN) {
  console.error("Falta API_TOKEN. El bot no se iniciará sin un token seguro.");
  process.exit(1);
}

if (INCOMING_WEBHOOK_URL && !INCOMING_WEBHOOK_TOKEN) {
  console.error("INCOMING_WEBHOOK_URL está configurado, pero falta INCOMING_WEBHOOK_TOKEN.");
  process.exit(1);
}

// -----------------------------------------------------------------------------
// Base de datos local
// -----------------------------------------------------------------------------

// SQLite vive en el mismo Droplet. Solo guardamos datos normalizados del mensaje;
// no guardamos imágenes, audios ni el JSON completo de Baileys para no llenar el disco.
fs.mkdirSync(path.dirname(DATABASE_PATH), { recursive: true });
fs.mkdirSync(BACKUP_DIR, { recursive: true });

const database = new DatabaseSync(DATABASE_PATH);

// WAL permite que una lectura ocurra mientras el bot escribe un mensaje.
database.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA busy_timeout = 5000;

  CREATE TABLE IF NOT EXISTS conversations (
    jid TEXT PRIMARY KEY,
    phone TEXT,
    contact_name TEXT,
    is_group INTEGER NOT NULL DEFAULT 0,
    last_message_preview TEXT,
    last_message_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    whatsapp_message_id TEXT NOT NULL UNIQUE,
    jid TEXT NOT NULL,
    sender_jid TEXT,
    sender_name TEXT,
    direction TEXT NOT NULL CHECK (direction IN ('incoming', 'outgoing')),
    message_type TEXT NOT NULL,
    message_text TEXT,
    media_mime_type TEXT,
    media_file_name TEXT,
    media_size_bytes INTEGER,
    message_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (jid) REFERENCES conversations(jid)
  );

  CREATE INDEX IF NOT EXISTS idx_messages_jid_message_at
    ON messages (jid, message_at DESC);

  CREATE INDEX IF NOT EXISTS idx_conversations_updated_at
    ON conversations (updated_at DESC);

  CREATE TABLE IF NOT EXISTS jid_aliases (
    alias_jid TEXT PRIMARY KEY,
    canonical_jid TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE INDEX IF NOT EXISTS idx_jid_aliases_canonical
    ON jid_aliases (canonical_jid);
`);

// Estas migraciones pequeñas permiten actualizar una base creada por una versión
// anterior del bot sin tener que borrarla.
ensureColumn("messages", "media_mime_type", "TEXT");
ensureColumn("messages", "media_file_name", "TEXT");
ensureColumn("messages", "media_size_bytes", "INTEGER");

function readPositiveInteger(value, fallback) {
  const parsed = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function ensureColumn(tableName, columnName, definition) {
  const columns = database.prepare(`PRAGMA table_info(${tableName})`).all();
  if (columns.some((column) => column.name === columnName)) return;

  database.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
}

function isoNow() {
  return new Date().toISOString();
}

function phoneFromJid(jid) {
  if (!jid || jid.endsWith("@g.us") || jid.endsWith("@broadcast")) return null;

  const number = jid.split("@")[0].replace(/\D/g, "");
  return number || null;
}

function rememberJidAlias(aliasJid, canonicalJid) {
  database
    .prepare(
      `
      INSERT INTO jid_aliases (alias_jid, canonical_jid, updated_at)
      VALUES (?, ?, ?)
      ON CONFLICT(alias_jid) DO UPDATE SET
        canonical_jid = excluded.canonical_jid,
        updated_at = excluded.updated_at
    `,
    )
    .run(aliasJid, canonicalJid, isoNow());
}

function canonicalJidForAlias(jid) {
  return (
    database.prepare("SELECT canonical_jid FROM jid_aliases WHERE alias_jid = ?").get(jid)
      ?.canonical_jid || jid
  );
}

function migrateConversationJid(aliasJid, canonicalJid) {
  if (
    !aliasJid?.endsWith("@lid") ||
    !canonicalJid?.endsWith("@s.whatsapp.net") ||
    aliasJid === canonicalJid
  ) {
    return false;
  }

  const source = database.prepare("SELECT * FROM conversations WHERE jid = ?").get(aliasJid);
  if (!source) {
    rememberJidAlias(aliasJid, canonicalJid);
    return false;
  }

  const target = database.prepare("SELECT * FROM conversations WHERE jid = ?").get(canonicalJid);
  database.exec("BEGIN IMMEDIATE");
  try {
    if (target) {
      const sourceIsNewer =
        !target.last_message_at ||
        (source.last_message_at && source.last_message_at > target.last_message_at);
      database
        .prepare(
          `
          UPDATE conversations
          SET phone = ?,
              contact_name = ?,
              last_message_preview = ?,
              last_message_at = ?,
              updated_at = ?
          WHERE jid = ?
        `,
        )
        .run(
          phoneFromJid(canonicalJid),
          target.contact_name || source.contact_name || null,
          sourceIsNewer ? source.last_message_preview : target.last_message_preview,
          sourceIsNewer ? source.last_message_at : target.last_message_at,
          isoNow(),
          canonicalJid,
        );
    } else {
      database
        .prepare(
          `
          INSERT INTO conversations (
            jid, phone, contact_name, is_group, last_message_preview,
            last_message_at, created_at, updated_at
          )
          VALUES (?, ?, ?, 0, ?, ?, ?, ?)
        `,
        )
        .run(
          canonicalJid,
          phoneFromJid(canonicalJid),
          source.contact_name || null,
          source.last_message_preview || null,
          source.last_message_at || null,
          source.created_at || isoNow(),
          isoNow(),
        );
    }

    database.prepare("UPDATE messages SET jid = ? WHERE jid = ?").run(canonicalJid, aliasJid);
    rememberJidAlias(aliasJid, canonicalJid);
    database.prepare("DELETE FROM conversations WHERE jid = ?").run(aliasJid);
    database.exec("COMMIT");
    return true;
  } catch (error) {
    database.exec("ROLLBACK");
    throw error;
  }
}

function applyLidPhoneMapping(value) {
  const mapping = extractLidPhoneMapping(value);
  if (!mapping) return false;
  migrateConversationJid(mapping.lid, mapping.pn);
  return true;
}

function applyLidPhoneMappings(values) {
  for (const value of values || []) applyLidPhoneMapping(value);
}

function aliasesForJid(jid) {
  return database
    .prepare("SELECT alias_jid FROM jid_aliases WHERE canonical_jid = ?")
    .all(jid)
    .map((row) => row.alias_jid);
}

function upsertConversation({ jid, contactName, messageType, messageText, messageAt }) {
  const now = isoNow();
  const preview = messageText ? messageText.slice(0, 300) : `[${messageType}]`;

  database
    .prepare(
      `
      INSERT INTO conversations (
        jid,
        phone,
        contact_name,
        is_group,
        last_message_preview,
        last_message_at,
        created_at,
        updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(jid) DO UPDATE SET
        phone = excluded.phone,
        contact_name = COALESCE(excluded.contact_name, conversations.contact_name),
        last_message_preview = excluded.last_message_preview,
        last_message_at = excluded.last_message_at,
        updated_at = excluded.updated_at
    `,
    )
    .run(
      jid,
      phoneFromJid(jid),
      contactName || null,
      jid.endsWith("@g.us") ? 1 : 0,
      preview,
      messageAt,
      now,
      now,
    );
}

function saveMessage({
  whatsappMessageId,
  jid,
  senderJid,
  senderName,
  direction,
  messageType,
  messageText,
  mediaMimeType,
  mediaFileName,
  mediaSizeBytes,
  messageAt,
}) {
  upsertConversation({
    jid,
    contactName: senderName,
    messageType,
    messageText,
    messageAt,
  });

  const result = database
    .prepare(
      `
      INSERT OR IGNORE INTO messages (
        whatsapp_message_id,
        jid,
        sender_jid,
        sender_name,
        direction,
        message_type,
        message_text,
        media_mime_type,
        media_file_name,
        media_size_bytes,
        message_at,
        created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
    )
    .run(
      whatsappMessageId,
      jid,
      senderJid || null,
      senderName || null,
      direction,
      messageType,
      messageText || null,
      mediaMimeType || null,
      mediaFileName || null,
      mediaSizeBytes || null,
      messageAt,
      isoNow(),
    );

  // changes = 0 significa que Baileys volvió a entregar un mensaje ya guardado.
  return result.changes > 0;
}

function parseLimit(value, defaultValue = 50) {
  const parsed = Number.parseInt(String(value || defaultValue), 10);
  if (!Number.isFinite(parsed)) return defaultValue;
  return Math.min(Math.max(parsed, 1), 200);
}

let backupInProgress = false;

async function createDatabaseBackup() {
  if (backupInProgress) return;
  backupInProgress = true;

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const destination = path.join(BACKUP_DIR, `whatsapp-${timestamp}.sqlite`);

  try {
    await backup(database, destination);
    removeOldBackups();
    console.log(`[BACKUP] Base de datos respaldada en ${destination}`);
  } catch (error) {
    console.error(
      "[BACKUP] No se pudo respaldar SQLite:",
      error instanceof Error ? error.message : error,
    );
  } finally {
    backupInProgress = false;
  }
}

function removeOldBackups() {
  const files = fs
    .readdirSync(BACKUP_DIR)
    .filter((file) => file.startsWith("whatsapp-") && file.endsWith(".sqlite"))
    .map((file) => ({
      file,
      modifiedAt: fs.statSync(path.join(BACKUP_DIR, file)).mtimeMs,
    }))
    .sort((a, b) => b.modifiedAt - a.modifiedAt);

  for (const oldBackup of files.slice(BACKUP_RETENTION_COUNT)) {
    fs.unlinkSync(path.join(BACKUP_DIR, oldBackup.file));
  }
}

function scheduleDatabaseBackups() {
  // Primer respaldo poco después de iniciar; luego uno cada 24 horas por defecto.
  const initialBackup = setTimeout(createDatabaseBackup, 5000);
  initialBackup.unref();

  const interval = setInterval(createDatabaseBackup, BACKUP_INTERVAL_HOURS * 60 * 60 * 1000);
  interval.unref();
}

scheduleDatabaseBackups();

// -----------------------------------------------------------------------------
// Lectura de mensajes de Baileys
// -----------------------------------------------------------------------------

function unwrapMessageContent(content) {
  let current = content;

  // Estos contenedores aparecen cuando WhatsApp envía mensajes efímeros o de una
  // sola visualización. Los quitamos para leer el contenido real.
  while (
    current?.ephemeralMessage?.message ||
    current?.viewOnceMessage?.message ||
    current?.viewOnceMessageV2?.message
  ) {
    current =
      current.ephemeralMessage?.message ||
      current.viewOnceMessage?.message ||
      current.viewOnceMessageV2?.message;
  }

  return current || {};
}

function extractMessageData(message) {
  const content = unwrapMessageContent(message?.message);
  const messageType = Object.keys(content)[0] || "unknown";
  const media =
    content.imageMessage ||
    content.videoMessage ||
    content.documentMessage ||
    content.audioMessage ||
    content.stickerMessage ||
    null;

  const messageText =
    content.conversation ||
    content.extendedTextMessage?.text ||
    content.imageMessage?.caption ||
    content.videoMessage?.caption ||
    content.documentMessage?.caption ||
    content.buttonsResponseMessage?.selectedDisplayText ||
    content.listResponseMessage?.title ||
    content.templateButtonReplyMessage?.selectedDisplayText ||
    null;

  return {
    messageType,
    messageText: typeof messageText === "string" ? messageText.trim() : null,
    // Guardamos solo metadatos. El archivo multimedia nunca entra en SQLite.
    mediaMimeType: media?.mimetype || null,
    mediaFileName: media?.fileName || null,
    mediaSizeBytes: protobufNumber(media?.fileLength),
  };
}

function protobufNumber(value) {
  if (value === undefined || value === null) return null;

  const parsed = typeof value === "object" ? Number(value.low ?? value) : Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

function messageTimestampToIso(value) {
  // Baileys puede entregar el timestamp como número o como Long de protobuf.
  const seconds = protobufNumber(value);

  if (!Number.isFinite(seconds) || seconds <= 0) return isoNow();
  return new Date(seconds * 1000).toISOString();
}

function normalizeJid(phoneOrJid) {
  const value = String(phoneOrJid || "").trim();
  if (!value) return "";

  if (value.includes("@")) return value;

  const digits = value.replace(/\D/g, "");
  return digits ? `${digits}@s.whatsapp.net` : "";
}

// -----------------------------------------------------------------------------
// Webhook opcional hacia el proyecto web
// -----------------------------------------------------------------------------

function wait(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function forwardIncomingMessage(record) {
  if (!INCOMING_WEBHOOK_URL) return;

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);

    try {
      const response = await fetch(INCOMING_WEBHOOK_URL, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${INCOMING_WEBHOOK_TOKEN}`,
          "Content-Type": "application/json",
          "X-Webhook-Event": "whatsapp.message.received",
        },
        body: JSON.stringify({
          event: "whatsapp.message.received",
          source: "andes-travel-whatsapp-bot",
          message: record,
        }),
        signal: controller.signal,
      });

      if (response.ok) return;

      console.warn(
        `[WEBHOOK] El proyecto respondió HTTP ${response.status} (intento ${attempt}/3).`,
      );
    } catch (error) {
      console.warn(
        `[WEBHOOK] No se pudo contactar al proyecto (intento ${attempt}/3):`,
        error instanceof Error ? error.message : error,
      );
    } finally {
      clearTimeout(timeout);
    }

    if (attempt < 3) await wait(attempt * 1000);
  }

  console.error(
    `[WEBHOOK] Mensaje ${record.whatsappMessageId} guardado localmente, pero no llegó al proyecto.`,
  );
}

// -----------------------------------------------------------------------------
// Conexión a WhatsApp
// -----------------------------------------------------------------------------

let sock = null;
let currentQrBase64 = null;
let status = "disconnected"; // disconnected | qr | connected

const logger = pino({ level: "silent" });

async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState("auth_info_baileys");
  const { version, isLatest } = await fetchLatestBaileysVersion();

  console.log(`Usando WA v${version.join(".")}, isLatest: ${isLatest}`);

  sock = makeWASocket({
    version,
    logger,
    printQRInTerminal: true,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger),
    },
    generateHighQualityLinkPreview: true,
  });

  sock.ev.on("creds.update", saveCreds);

  // WhatsApp puede entregar la relación LID ↔ teléfono en distintos eventos.
  // La guardamos apenas aparece, incluso antes de que llegue el primer mensaje.
  sock.ev.on("lid-mapping.update", (mapping) => {
    applyLidPhoneMapping(mapping);
  });

  sock.ev.on("contacts.upsert", (contacts) => {
    applyLidPhoneMappings(contacts);
  });

  sock.ev.on("contacts.update", (contacts) => {
    applyLidPhoneMappings(contacts);
  });

  sock.ev.on("messaging-history.set", ({ contacts, lidPnMappings }) => {
    applyLidPhoneMappings(lidPnMappings);
    applyLidPhoneMappings(contacts);
  });

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      status = "qr";
      currentQrBase64 = await QRCode.toDataURL(qr);
      console.log("Nuevo QR generado. Escanéalo en el sistema.");
    }

    if (connection === "close") {
      const shouldReconnect =
        lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;

      console.log(
        "Conexión cerrada por:",
        lastDisconnect?.error,
        ", reconectando:",
        shouldReconnect,
      );

      if (shouldReconnect) {
        status = "disconnected";
        currentQrBase64 = null;
        setTimeout(connectToWhatsApp, 2000);
      } else {
        console.log(
          "Sesión cerrada. Borra auth_info_baileys y reinicia para escanear un nuevo QR.",
        );
        fs.rmSync("auth_info_baileys", { recursive: true, force: true });
        status = "disconnected";
        currentQrBase64 = null;
        setTimeout(connectToWhatsApp, 2000);
      }
    } else if (connection === "open") {
      console.log("WhatsApp conectado exitosamente");
      status = "connected";
      currentQrBase64 = null;
    }
  });

  // Guardar y reenviar los mensajes recibidos.
  sock.ev.on("messages.upsert", async (event) => {
    if (event.type !== "notify") return;

    for (const message of event.messages || []) {
      try {
        const rawJid = message?.key?.remoteJid || "";

        // No procesar mensajes propios ni mensajes de estado de WhatsApp.
        if (!rawJid || message.key?.fromMe || rawJid === "status@broadcast") continue;

        const resolvedJid = await resolveMessageJid(
          message.key,
          sock?.signalRepository?.lidMapping,
        );
        const jid = canonicalJidForAlias(resolvedJid);
        const senderJid = resolveSenderJid(message.key, jid);
        if (rawJid !== jid) migrateConversationJid(rawJid, jid);

        const { messageType, messageText, mediaMimeType, mediaFileName, mediaSizeBytes } =
          extractMessageData(message);
        const messageAt = messageTimestampToIso(message.messageTimestamp);
        const whatsappMessageId = message.key?.id || `incoming-${randomUUID()}`;

        const record = {
          whatsappMessageId,
          jid,
          phone: phoneFromJid(jid),
          senderJid,
          senderName: message.pushName || null,
          direction: "incoming",
          messageType,
          messageText,
          mediaMimeType,
          mediaFileName,
          mediaSizeBytes,
          messageAt,
        };

        const wasSaved = saveMessage(record);
        if (!wasSaved) continue;

        console.log(
          `[INCOMING] ${record.phone || record.jid}: ${record.messageText || `[${record.messageType}]`}`,
        );

        await forwardIncomingMessage(record);
      } catch (error) {
        console.error("Error procesando mensaje entrante:", error);
      }
    }
  });
}

connectToWhatsApp().catch((error) => {
  console.error("No se pudo iniciar WhatsApp:", error);
  process.exit(1);
});

// -----------------------------------------------------------------------------
// API HTTP del bot
// -----------------------------------------------------------------------------

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

function authMiddleware(req, res, next) {
  const token = req.headers["x-api-token"];

  if (!token || token !== API_TOKEN) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  next();
}

function sendAndStore(jid, content, details) {
  return sock.sendMessage(jid, content).then((sentMessage) => {
    try {
      saveMessage({
        whatsappMessageId: sentMessage?.key?.id || `outgoing-${randomUUID()}`,
        jid,
        senderJid: null,
        senderName: null,
        direction: "outgoing",
        messageType: details.messageType,
        messageText: details.messageText,
        mediaMimeType: details.mediaMimeType,
        mediaFileName: details.mediaFileName,
        mediaSizeBytes: details.mediaSizeBytes,
        messageAt: isoNow(),
      });
    } catch (error) {
      // El historial local es secundario: no debemos informar error si WhatsApp
      // ya entregó el mensaje. El CRM seguirá registrando su propio historial.
      console.warn(
        "[SQLITE] No se pudo guardar el mensaje saliente localmente:",
        error instanceof Error ? error.message : error,
      );
    }

    return sentMessage;
  });
}

app.get("/", (req, res) => {
  res.send("Andes Travel WhatsApp Bot OK");
});

// El estado y el QR también requieren autenticación.
app.get("/status", authMiddleware, (req, res) => {
  res.json({
    status,
    qr: currentQrBase64,
  });
});

// Lista de conversaciones para que la web pueda construir una bandeja de entrada.
app.get("/conversations", authMiddleware, async (req, res) => {
  const limit = parseLimit(req.query.limit);
  const initialConversations = database
    .prepare(
      `
      SELECT *
      FROM conversations
      ORDER BY updated_at DESC
      LIMIT ?
    `,
    )
    .all(limit);

  for (const conversation of initialConversations) {
    if (!conversation.jid.endsWith("@lid")) continue;
    const resolvedJid = await resolveMessageJid(
      { remoteJid: conversation.jid },
      sock?.signalRepository?.lidMapping,
    );
    if (resolvedJid !== conversation.jid) {
      migrateConversationJid(conversation.jid, resolvedJid);
    }
  }

  const conversations = database
    .prepare(
      `
      SELECT *
      FROM conversations
      ORDER BY updated_at DESC
      LIMIT ?
    `,
    )
    .all(limit)
    .map((conversation) => ({
      ...conversation,
      alias_jids: aliasesForJid(conversation.jid),
    }));

  res.json({ conversations });
});

// Vinculación manual para LID históricos que WhatsApp no puede resolver.
app.post("/conversations/link", authMiddleware, (req, res) => {
  const aliasJid = String(req.body?.aliasJid || "").trim();
  const canonicalJid = normalizeJid(req.body?.phone);
  if (!aliasJid.endsWith("@lid") || !canonicalJid.endsWith("@s.whatsapp.net")) {
    return res.status(400).json({ error: "Alias o teléfono inválido" });
  }

  migrateConversationJid(aliasJid, canonicalJid);
  return res.json({ success: true, jid: canonicalJid });
});

// Historial de una conversación. El JID debe enviarse URL-encoded.
app.get("/conversations/:jid/messages", authMiddleware, (req, res) => {
  const jid = decodeURIComponent(req.params.jid);
  const limit = parseLimit(req.query.limit);

  const messages = database
    .prepare(
      `
      SELECT *
      FROM messages
      WHERE jid = ?
      ORDER BY message_at DESC
      LIMIT ?
    `,
    )
    .all(jid, limit)
    .reverse();

  res.json({ jid, messages });
});

app.post("/send", authMiddleware, async (req, res) => {
  if (status !== "connected" || !sock) {
    return res.status(503).json({ error: "WhatsApp no está conectado" });
  }

  try {
    const { action, phone, message, url, fileName, caption } = req.body;
    const jid = normalizeJid(phone);

    if (!jid) return res.status(400).json({ error: "Teléfono requerido" });

    if (action === "send-text") {
      const text = message || "";
      const sentMessage = await sendAndStore(
        jid,
        { text },
        { messageType: "conversation", messageText: text },
      );
      return res.json({
        success: true,
        message: "Texto enviado",
        whatsappMessageId: sentMessage?.key?.id || null,
      });
    }

    if (action === "send-image") {
      if (!url) return res.status(400).json({ error: "Falta url" });

      const sentMessage = await sendAndStore(
        jid,
        { image: { url }, caption: caption || "" },
        {
          messageType: "imageMessage",
          messageText: caption || null,
          mediaMimeType: "image/remote",
        },
      );
      return res.json({
        success: true,
        message: "Imagen enviada",
        whatsappMessageId: sentMessage?.key?.id || null,
      });
    }

    if (action === "send-file") {
      if (!url) return res.status(400).json({ error: "Falta url" });

      const sentMessage = await sendAndStore(
        jid,
        {
          document: { url },
          fileName: fileName || "documento.pdf",
          mimetype: "application/pdf",
          caption: caption || "",
        },
        {
          messageType: "documentMessage",
          messageText: caption || null,
          mediaMimeType: "application/pdf",
          mediaFileName: fileName || "documento.pdf",
        },
      );
      return res.json({
        success: true,
        message: "Archivo enviado",
        whatsappMessageId: sentMessage?.key?.id || null,
      });
    }

    return res.status(400).json({ error: "Acción no soportada" });
  } catch (error) {
    console.error("Error enviando mensaje:", error);
    return res
      .status(500)
      .json({
        error: error instanceof Error ? error.message : "Error interno enviando el mensaje",
      });
  }
});

app.listen(PORT, () => {
  console.log(`Servidor Express escuchando en el puerto ${PORT}`);
  console.log(`Base de datos SQLite: ${DATABASE_PATH}`);
  console.log(`Respaldos SQLite: ${BACKUP_DIR} (se conservan ${BACKUP_RETENTION_COUNT})`);
  console.log(
    INCOMING_WEBHOOK_URL
      ? `Webhook de mensajes entrantes: ${INCOMING_WEBHOOK_URL}`
      : "Webhook de mensajes entrantes: no configurado todavía",
  );
});
