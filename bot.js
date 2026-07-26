import express from 'express';
import cors from 'cors';
import fs from 'fs';
import { makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion } from '@whiskeysockets/baileys';
import pino from 'pino';
import QRCode from 'qrcode';

const PORT = process.env.PORT || 3000;
const API_TOKEN = process.env.API_TOKEN || "TOKEN_SECRETO_AQUI";

const app = express();
app.use(cors());
app.use(express.json());

let sock = null;
let currentQrBase64 = null;
let status = 'disconnected'; // 'disconnected' | 'qr' | 'connected'

const logger = pino({ level: 'silent' }); // Cambiar a 'info' para depurar

async function connectToWhatsApp() {
    const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');
    const { version, isLatest } = await fetchLatestBaileysVersion();
    
    console.log(`Usando WA v${version.join('.')}, isLatest: ${isLatest}`);

    sock = makeWASocket({
        version,
        logger,
        printQRInTerminal: true, // También lo mostramos en terminal por si acaso
        auth: state,
        generateHighQualityLinkPreview: true,
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;
        
        if (qr) {
            status = 'qr';
            currentQrBase64 = await QRCode.toDataURL(qr);
            console.log("Nuevo QR generado. Escanéalo en el sistema.");
        }

        if (connection === 'close') {
            const shouldReconnect = lastDisconnect.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log('Conexión cerrada por:', lastDisconnect.error, ', reconectando:', shouldReconnect);
            
            if (shouldReconnect) {
                status = 'disconnected';
                currentQrBase64 = null;
                setTimeout(connectToWhatsApp, 2000);
            } else {
                console.log('Sesión cerrada. Borra la carpeta auth_info_baileys y reinicia para escanear un nuevo QR.');
                fs.rmSync('auth_info_baileys', { recursive: true, force: true });
                status = 'disconnected';
                currentQrBase64 = null;
                setTimeout(connectToWhatsApp, 2000);
            }
        } else if (connection === 'open') {
            console.log('✅ WhatsApp conectado exitosamente');
            status = 'connected';
            currentQrBase64 = null;
        }
    });

    // Manejar mensajes entrantes si fuera necesario
    sock.ev.on('messages.upsert', async m => {
        // Por ahora ignoramos los mensajes entrantes
    });
}

connectToWhatsApp();

// Middleware de autenticación
function authMiddleware(req, res, next) {
    const token = req.headers['x-api-token'];
    if (!token || token !== API_TOKEN) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
}

app.get('/', (req, res) => {
    res.send('Andes Travel WhatsApp Bot OK');
});

// Endpoint público/privado para leer el estado y QR
app.get('/status', (req, res) => {
    // Si queremos protegerlo: authMiddleware(req, res, () => res.json(...))
    // Pero es más útil dejarlo abierto para que el panel lo lea fácil, 
    // o protegerlo igual con el token. Protegerlo es mejor.
    res.json({
        status,
        qr: currentQrBase64
    });
});

app.post('/send', authMiddleware, async (req, res) => {
    if (status !== 'connected' || !sock) {
        return res.status(503).json({ error: 'WhatsApp no está conectado' });
    }

    try {
        const { action, phone, message, url, fileName, caption } = req.body;
        
        if (!phone) return res.status(400).json({ error: 'Teléfono requerido' });
        
        // Formatear el teléfono para Baileys (agregar @s.whatsapp.net si es número)
        let jid = phone;
        if (!jid.includes('@')) {
            jid = jid.replace(/\D/g, '') + '@s.whatsapp.net';
        }

        if (action === 'send-text') {
            await sock.sendMessage(jid, { text: message || '' });
            return res.json({ success: true, message: 'Texto enviado' });
            
        } else if (action === 'send-image') {
            if (!url) return res.status(400).json({ error: 'Falta url' });
            await sock.sendMessage(jid, { 
                image: { url }, 
                caption: caption || '' 
            });
            return res.json({ success: true, message: 'Imagen enviada' });
            
        } else if (action === 'send-file') {
            if (!url) return res.status(400).json({ error: 'Falta url' });
            await sock.sendMessage(jid, { 
                document: { url }, 
                fileName: fileName || 'documento.pdf',
                mimetype: 'application/pdf', // Asumimos PDF, se podría hacer dinámico
                caption: caption || ''
            });
            return res.json({ success: true, message: 'Archivo enviado' });
            
        } else {
            return res.status(400).json({ error: 'Acción no soportada' });
        }

    } catch (error) {
        console.error('Error enviando mensaje:', error);
        res.status(500).json({ error: error.message || 'Error interno enviando el mensaje' });
    }
});

app.listen(PORT, () => {
    console.log(`🚀 Servidor Express escuchando en el puerto ${PORT}`);
    console.log(`👉 Asegúrate de definir API_TOKEN en las variables de entorno o en el código.`);
});
