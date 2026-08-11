export function isPhoneJid(value) {
  return typeof value === "string" && value.endsWith("@s.whatsapp.net");
}

export function normalizePhoneJid(value) {
  if (!value) return "";
  const jid = String(value).trim();
  if (isPhoneJid(jid)) return jid;
  const digits = jid.replace(/\D/g, "");
  return digits ? `${digits}@s.whatsapp.net` : "";
}

export function extractLidPhoneMapping(value) {
  if (!value || typeof value !== "object") return null;

  const lid = [value.lid, value.id, value.jid].find(
    (candidate) => typeof candidate === "string" && candidate.endsWith("@lid"),
  );
  const pn = [value.pn, value.phoneNumber, value.id, value.jid].find(isPhoneJid);

  return lid && pn ? { lid, pn: normalizePhoneJid(pn) } : null;
}

export async function resolveMessageJid(key, lidMapping) {
  const rawJid = String(key?.remoteJid || "").trim();
  if (!rawJid.endsWith("@lid")) return rawJid;

  const directPhoneJid = [
    key?.remoteJidAlt,
    key?.participantPn,
    key?.senderPn,
    key?.participantAlt,
  ].find(isPhoneJid);
  if (directPhoneJid) return directPhoneJid;

  if (typeof lidMapping?.getPNForLID !== "function") return rawJid;

  try {
    return normalizePhoneJid(await lidMapping.getPNForLID(rawJid)) || rawJid;
  } catch {
    return rawJid;
  }
}

export function resolveSenderJid(key, fallbackJid) {
  return (
    [key?.participantPn, key?.senderPn, key?.participantAlt, key?.participant].find(
      (value) => typeof value === "string" && value.trim(),
    ) || fallbackJid
  );
}
