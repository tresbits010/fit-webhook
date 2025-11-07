// server.js
// ======================================================
// Webhook / Backend FitSuite Pro - LICENCIAS + MP OAuth
// → Esquema referidos alineado con Program.cs (Cloud Run)
// - Fuente de verdad licencia: gimnasios/{gymId}/licencia/datos (v2 + rev++)
// - Caché cliente escritorio: gimnasios/{gymId}/config/config
// - Referidos:
//   * Índice global: referralCodes/{CODE} -> { gymId }
//   * Por gym: gimnasios/{gymId}/referrals/config { myCode, totalReferrals, discountTier(0..20), … }
//   * Registro de uso en signup: gimnasios/{buyerGym}/referrals/applied_pending { usedCode, referrerGymId, status:"pending" }
//   * Webhook licencia: consume pending, acredita al referrer, actualiza tier/points e historial
//   * Email al REFERIDOR en pago aprobado (idempotente por paymentId)
// Requiere: Node 18+, express, firebase-admin, mercadopago, dotenv, nodemailer
// Vars SMTP: SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM, BRAND_NAME (opcional)
// ======================================================

const express = require('express');
const mercadopago = require('mercadopago'); // SOLO para licencias (tu cuenta)
const admin = require('firebase-admin');
const dotenv = require('dotenv');
const nodemailer = require('nodemailer');
dotenv.config();

const app = express();
app.use(express.json());

// ==============================
//  Firebase Admin
// ==============================
if (!process.env.FIREBASE_CREDENTIALS) {
  throw new Error('Falta FIREBASE_CREDENTIALS (JSON) en variables de entorno');
}
const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

// ==============================
//  MP SDK (para LICENCIAS)
// ==============================
if (!process.env.MP_ACCESS_TOKEN) {
  console.warn('⚠️ Falta MP_ACCESS_TOKEN (usado para leer pagos de LICENCIAS con tu cuenta)');
}
mercadopago.configure({ access_token: process.env.MP_ACCESS_TOKEN });

// ==============================
//  Helpers generales
// ==============================
function normalize(s) {
  return (s || '').toString().trim().toLowerCase();
}
function nowTs() {
  return FieldValue.serverTimestamp();
}
const BRAND = process.env.BRAND_NAME || 'FitSuite Pro';

// 🔎 Zona horaria a usar para “cierre de día”
const BA_TZ = 'America/Argentina/Buenos_Aires';

// ID de día en TZ Buenos Aires (YYYY-MM-DD)
function dayId(date = new Date(), timeZone = BA_TZ) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(date);
  const y = parts.find(p => p.type === 'year')?.value;
  const m = parts.find(p => p.type === 'month')?.value;
  const d = parts.find(p => p.type === 'day')?.value;
  return `${y}-${m}-${d}`;
}

function resumenDiaRef(gymId, d = new Date()) {
  return db.doc(`gimnasios/${gymId}/resumen_dias/${dayId(d, BA_TZ)}`);
}

// mapea método → 'efectivo' | 'online'
function medioKeyFrom(method) {
  const m = normalize(method);
  if (m === 'efectivo' || m === 'cash') return 'efectivo';
  return 'online';
}

/**
 * Acumula ingreso diario (en transacción):
 * tipo: 'altas' | 'renovaciones' | 'tienda'
 * medio: 'efectivo' | 'online'
 */
function acumularIngresoDiarioTx(tx, gymId, tipo, monto, medio) {
  const ref = resumenDiaRef(gymId);
  const init = {
    fecha: dayId(new Date(), BA_TZ),
    ingresos: {
      altas:        { cantidad: 0, total: 0, efectivo: 0, online: 0 },
      renovaciones: { cantidad: 0, total: 0, efectivo: 0, online: 0 },
      tienda:       { cantidad: 0, total: 0, efectivo: 0, online: 0 }
    },
    gastos: { cantidad: 0, total: 0, efectivo: 0, online: 0 },
    ultimaActualizacion: nowTs()
  };
  tx.set(ref, init, { merge: true });
  const updates = {
    [`ingresos.${tipo}.cantidad`]: FieldValue.increment(1),
    [`ingresos.${tipo}.total`]: FieldValue.increment(Number(monto || 0)),
    [`ingresos.${tipo}.${medio}`]: FieldValue.increment(Number(monto || 0)),
    ultimaActualizacion: nowTs()
  };
  tx.set(ref, updates, { merge: true });
}

// ==============================
//  Helpers de Plan (normalización)
// ==============================
async function readPlanById(planId) {
  // lee desde 'licencias' y si no existe, fallback a 'planesLicencia'
  let planSnap = await db.collection('licencias').doc(planId).get();
  if (!planSnap.exists) planSnap = await db.collection('planesLicencia').doc(planId).get();
  if (!planSnap.exists) throw new Error('Plan no encontrado');
  const plan = planSnap.data() || {};
  return { id: planId, ...plan };
}

function normalizePlanModules(plan) {
  // admite: modulosPlan (objeto o array), modulos, modules, features
  const out = {};
  const candidates = ['modulosPlan', 'modulos', 'modules', 'features'];
  for (const key of candidates) {
    const v = plan[key];
    if (!v) continue;
    if (Array.isArray(v)) {
      for (const item of v) {
        const s = (item || '').toString().trim();
        if (s) out[s] = true;
      }
    } else if (typeof v === 'object') {
      for (const [k, val] of Object.entries(v)) {
        if (!k) continue;
        out[k] = (typeof val === 'boolean') ? val : !!val;
      }
    }
  }
  return out; // mapa { feature: true/false }
}

function normalizePlanLimits(plan, fallbackMaxUsuarios = 0) {
  // Soporta raíz o anidado en limits.*
  const lim = Object.assign({}, plan.limits || {});
  const norm = {
    maxMembers:      Number(lim.maxMembers      ?? plan.maxMembers ?? plan.maxUsuarios ?? fallbackMaxUsuarios ?? 0),
    maxDevices:      Number(lim.maxDevices      ?? plan.maxDevices ?? 1),
    maxBranches:     Number(lim.maxBranches     ?? plan.maxBranches ?? 1),
    maxOfflineHours: Number(lim.maxOfflineHours ?? plan.maxOfflineHours ?? 168)
  };
  if (Number.isNaN(norm.maxDevices) || norm.maxDevices < 1) norm.maxDevices = 1;
  if (Number.isNaN(norm.maxBranches) || norm.maxBranches < 1) norm.maxBranches = 1;
  if (Number.isNaN(norm.maxOfflineHours) || norm.maxOfflineHours < 24) norm.maxOfflineHours = 24;
  if (Number.isNaN(norm.maxMembers) || norm.maxMembers < 0) norm.maxMembers = 0;
  return norm;
}

// ==============================
//  MAILER (SMTP) + HTML
// ==============================
function makeMailer() {
  const host = process.env.SMTP_HOST;
  const port = Number(process.env.SMTP_PORT || 587);
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  const from = process.env.SMTP_FROM || `no-reply@${(host || 'example.com').replace(/^smtp\./, '')}`;

  if (!host || !user || !pass) {
    console.warn('✉️  Mailer deshabilitado (faltan SMTP_HOST/SMTP_USER/SMTP_PASS). Se hará console.log del HTML.');
    return {
      async send({ to, subject, html }) {
        console.log('---- EMAIL (simulado) ----');
        console.log('To:', to);
        console.log('Subject:', subject);
        console.log(html);
        console.log('--------------------------');
        return { simulated: true };
      },
      from
    };
  }

  const transporter = nodemailer.createTransport({
    host,
    port,
    secure: port === 465,
    auth: { user, pass }
  });

  return {
    async send({ to, subject, html }) {
      return transporter.sendMail({ from, to, subject, html });
    },
    from
  };
}

function getReferralEmailHtml({ referrerGymName, buyerGymName, usedCode, paymentId }) {
  const accent = '#6366F1';
  return `
<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width">
<title>${BRAND} · Nuevo referido confirmado</title>
</head>
<body style="margin:0;background:#0f1220;font-family:Segoe UI,Roboto,Arial,sans-serif;color:#e9eefb">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="padding:32px 0">
    <tr>
      <td align="center">
        <table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background:#151936;border-radius:14px;box-shadow:0 10px 30px rgba(0,0,0,.35);overflow:hidden">
          <tr>
            <td style="padding:24px 28px;background:linear-gradient(135deg, ${accent}, #8b5cf6); color:#fff">
              <h1 style="margin:0;font-size:20px;letter-spacing:.3px">${BRAND}</h1>
              <p style="margin:6px 0 0 0;opacity:.95">Programa de referidos</p>
            </td>
          </tr>
          <tr>
            <td style="padding:28px">
              <h2 style="margin:0 0 12px 0;font-size:22px;color:#fff">¡Nuevo referido confirmado! 🎉</h2>
              <p style="margin:0 0 16px 0;line-height:1.55;color:#cfd6ee">
                El gimnasio <b style="color:#fff">${escapeHtml(buyerGymName)}</b> completó un pago usando tu código
                <b style="color:#fff">${escapeHtml(usedCode)}</b>.
              </p>
              <div style="margin:18px 0;padding:14px 16px;border:1px solid #2a2f52;border-radius:10px;background:#111530">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:14px;color:#cfd6ee">
                  <tr>
                    <td style="padding:4px 0;width:140px;color:#9aa3c7">Referidor</td>
                    <td style="padding:4px 0"><b style="color:#fff">${escapeHtml(referrerGymName)}</b></td>
                  </tr>
                  <tr>
                    <td style="padding:4px 0;color:#9aa3c7">Gimnasio referido</td>
                    <td style="padding:4px 0">${escapeHtml(buyerGymName)}</td>
                  </tr>
                  <tr>
                    <td style="padding:4px 0;color:#9aa3c7">Código usado</td>
                    <td style="padding:4px 0">${escapeHtml(usedCode)}</td>
                  </tr>
                  <tr>
                    <td style="padding:4px 0;color:#9aa3c7">ID de pago</td>
                    <td style="padding:4px 0">${escapeHtml(String(paymentId))}</td>
                  </tr>
                </table>
              </div>
              <p style="margin:0 0 18px 0;line-height:1.55;color:#cfd6ee">
                Tu <b style="color:#fff">descuento</b> para la próxima renovación aumentó y (si aplica) sumaste <b style="color:#fff">puntos de premio</b>.
              </p>
              <div style="text-align:center;margin:24px 0 8px">
                <a href="#" style="display:inline-block;background:${accent};color:#fff;text-decoration:none;padding:12px 18px;border-radius:10px;font-weight:600">Ver mi progreso</a>
              </div>
              <p style="margin:16px 0 0 0;font-size:12px;color:#9aa3c7">Este es un mensaje automático. Si tenés dudas, respondé este correo.</p>
            </td>
          </tr>
          <tr>
            <td style="padding:16px 28px;background:#0f122a;color:#9aa3c7;font-size:12px">
              © ${new Date().getFullYear()} ${BRAND}. Todos los derechos reservados.
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
`;
}

// helper seguro para HTML
function escapeHtml(s) {
  return String(s || '')
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'",'&#39;');
}

// ==============================
//  REFERIDOS — helpers (ALINEADO ESQUEMA NUEVO)
// ==============================

// Descuento por referidos del COMPRADOR: su propio discountTier (0..20)
async function getReferralDiscountPctForBuyer(gymId) {
  try {
    const snap = await db.doc(`gimnasios/${gymId}/referrals/config`).get();
    if (!snap.exists) return 0;
    const tier = Number(snap.data()?.discountTier ?? 0);
    if (Number.isNaN(tier)) return 0;
    return Math.max(0, Math.min(tier, 20));
  } catch {
    return 0;
  }
}

// Aplica crédito al REFERIDOR consumiendo el pending del COMPRADOR, idempotente por paymentId
// Además: escribe applied_approved (para lectura post-TX y envío de email)
async function applyReferralCreditInTx(tx, { buyerGymId, paymentId, planId }) {
  const buyerRef = db.doc(`gimnasios/${buyerGymId}`);
  const pendingRef = buyerRef.collection('referrals').doc('applied_pending');

  const pendingSnap = await tx.get(pendingRef);
  if (!pendingSnap.exists) return; // nada que consumir

  const p = pendingSnap.data() || {};
  if ((p.status || 'pending') !== 'pending') return; // ya consumido o ignorado

  const usedCode = p.usedCode;
  const referrerGymId = p.referrerGymId;

  if (!referrerGymId || typeof referrerGymId !== 'string') return;
  if (referrerGymId === buyerGymId) return; // guardrail

  // Historia del referrer por paymentId para idempotencia fuerte (además de _mp_processed)
  const refRoot    = db.doc(`gimnasios/${referrerGymId}`);
  const refCfgRef  = refRoot.collection('referrals').doc('config');
  const refHistRef = refRoot.collection('referrals').doc('history').collection('items').doc(String(paymentId));

  const histSnap = await tx.get(refHistRef);
  if (histSnap.exists) {
    // ya otorgado: solo marcar el pending como consumed y dejar aplicado
    tx.set(buyerRef.collection('referrals').doc('applied_approved'), {
      usedCode: usedCode || null,
      referrerGymId,
      approvedAt: nowTs(),
      paymentId: String(paymentId),
      planId
    }, { merge: true });

    tx.set(pendingRef, { status: 'consumed', consumedAt: nowTs(), paymentId: String(paymentId) }, { merge: true });
    return;
  }

  const cfgSnap = await tx.get(refCfgRef);
  const cfg = cfgSnap.exists ? (cfgSnap.data() || {}) : {};
  const prevTotal = Number(cfg.totalReferrals || 0);
  const newTotal  = prevTotal + 1;
  const newTier   = Math.min(20, newTotal * 4);

  // Actualizo contadores del referrer
  const cfgUpdates = {
    totalReferrals: FieldValue.increment(1),
    discountTier: newTier,
    updatedAt: nowTs()
  };
  // Bonus a partir del 6° referido confirmado
  if (newTotal >= 6) {
    cfgUpdates.totalPointsEarned = FieldValue.increment(100);
    cfgUpdates.pointsAvailable   = FieldValue.increment(100);
  }
  tx.set(refCfgRef, cfgUpdates, { merge: true });

  // Historial del referidor
  tx.set(refHistRef, {
    buyerGymId,
    paymentId: String(paymentId),
    planId,
    at: nowTs()
  }, { merge: true });

  // Consumir el pending y registrar aprobado en el COMPRADOR
  tx.set(buyerRef.collection('referrals').doc('applied_approved'), {
    usedCode: usedCode || null,
    referrerGymId,
    approvedAt: nowTs(),
    paymentId: String(paymentId),
    planId
  }, { merge: true });

  tx.set(pendingRef, {
    status: 'consumed',
    consumedAt: nowTs(),
    paymentId: String(paymentId),
    referrerGymId
  }, { merge: true });

  // Crear marca idempotente de email (en el referidor)
  const notifRef = refRoot.collection('referrals').doc('notifications')
                  .collection('emails').doc(String(paymentId));
  tx.set(notifRef, {
    buyerGymId,
    usedCode: usedCode || null,
    createdAt: nowTs(),
    sentEmail: false
  }, { merge: true });
}

// Enviar email al REFERIDOR (idempotente por paymentId)
async function sendReferralCongratsEmail({ referrerGymId, buyerGymId, usedCode, paymentId }) {
  const notifRef = db.doc(`gimnasios/${referrerGymId}/referrals/notifications/emails/${String(paymentId)}`);
  const notifSnap = await notifRef.get();
  if (notifSnap.exists && notifSnap.data()?.sentEmail === true) {
    return { ok: true, reason: 'EMAIL_ALREADY_SENT' };
  }

  // obtener admin del referidor
  const adminQ = await db.collection(`gimnasios/${referrerGymId}/usuarios`).where('rol', '==', 'admin').limit(1).get();
  if (adminQ.empty) {
    await notifRef.set({ failed: true, failReason: 'NO_ADMIN_EMAIL', checkedAt: nowTs() }, { merge: true });
    return { ok: false, reason: 'NO_ADMIN_EMAIL' };
  }
  const adminDoc = adminQ.docs[0].data();
  const to = adminDoc.email;
  if (!to) {
    await notifRef.set({ failed: true, failReason: 'NO_ADMIN_EMAIL', checkedAt: nowTs() }, { merge: true });
    return { ok: false, reason: 'NO_ADMIN_EMAIL' };
  }

  // nombres legibles
  const refGymSnap = await db.doc(`gimnasios/${referrerGymId}`).get();
  const buyerGymSnap = await db.doc(`gimnasios/${buyerGymId}`).get();

  const refName   = (refGymSnap.exists ? (refGymSnap.data()?.nombre) : null) || referrerGymId;
  const buyerName = (buyerGymSnap.exists ? (buyerGymSnap.data()?.nombre) : null) || buyerGymId;

  const mailer = makeMailer();
  const subject = `🎉 ¡Nuevo referido confirmado! ${buyerName} usó tu código`;
  const html = getReferralEmailHtml({
    referrerGymName: refName,
    buyerGymName: buyerName,
    usedCode: usedCode || '—',
    paymentId
  });

  try {
    await mailer.send({ to, subject, html });
    await notifRef.set({ sentEmail: true, sentAt: nowTs() }, { merge: true });
    return { ok: true, mailed: true };
  } catch (err) {
    await notifRef.set({ failed: true, failReason: String(err), checkedAt: nowTs() }, { merge: true });
    return { ok: false, reason: 'MAIL_ERROR', error: String(err) };
  }
}

// ==============================
//  OAuth / Tokens (gimnasios)
// ==============================
async function mpOAuthTokenExchange({ code, redirectUri }) {
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: process.env.MP_CLIENT_ID,
    client_secret: process.env.MP_CLIENT_SECRET,
    code,
    redirect_uri: redirectUri
  });
  const resp = await fetch('https://api.mercadopago.com/oauth/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body
  });
  if (!resp.ok) throw new Error(`OAuth exchange failed: ${resp.status} ${await resp.text()}`);
  return resp.json();
}

async function mpOAuthRefresh(refreshToken) {
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    client_id: process.env.MP_CLIENT_ID,
    client_secret: process.env.MP_CLIENT_SECRET,
    refresh_token: refreshToken
  });
  const resp = await fetch('https://api.mercadopago.com/oauth/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body
  });
  if (!resp.ok) throw new Error(`OAuth refresh failed: ${resp.status} ${await resp.text()}`);
  return resp.json();
}

async function mpGetUserMe(accessToken) {
  const resp = await fetch('https://api.mercadopago.com/users/me', {
    headers: { Authorization: `Bearer ${accessToken}` }
  });
  if (!resp.ok) throw new Error(`users/me failed: ${resp.status} ${await resp.text()}`);
  return resp.json();
}

// Devuelve un access_token válido del gym (refresca si va a vencer)
async function getValidGymAccessToken(gymId) {
  const ref = db.doc(`gimnasios/${gymId}/integraciones/mp`);
  const snap = await ref.get();
  if (!snap.exists) throw new Error('Gym sin integración MP');

  const data = snap.data() || {};
  let { access_token, refresh_token, expires_at } = data;

  const soon = Date.now() + 60 * 1000; // margen 60s
  if (!access_token || !expires_at || expires_at <= soon) {
    if (!refresh_token) throw new Error('No hay refresh_token para renovar');
    const tokenJson = await mpOAuthRefresh(refresh_token);
    access_token = tokenJson.access_token;
    refresh_token = tokenJson.refresh_token || refresh_token;
    const expiresIn = Number(tokenJson.expires_in || 0);
    expires_at = Date.now() + expiresIn * 1000;

    await ref.set(
      {
        access_token,
        refresh_token,
        expires_at,
        token_type: tokenJson.token_type || 'bearer',
        scope: tokenJson.scope || data.scope || null,
        updated_at: nowTs()
      },
      { merge: true }
    );
  }
  return access_token;
}

// =======================================================
//  LICENCIAS — procesar pago por paymentId (idempotente)
// =======================================================
async function processLicensePaymentById(paymentId) {
  try {
    const { body: payment } = await mercadopago.payment.get(paymentId);
    if (!payment || payment.status !== 'approved') {
      return { ok: false, reason: 'not_approved' };
    }

    const extRef = payment.external_reference || '';
    // external_reference: gym:{gymId}|plan:{planId}|ref:{...}|disc:{pct}
    const [gymPart, planPart] = extRef.split('|');
    const gimnasioId = gymPart?.split(':')[1];
    const planId     = planPart?.split(':')[1];

    if (!gimnasioId || !planId) {
      console.warn('external_reference inesperado:', extRef);
      return { ok: false, reason: 'bad_extref' };
    }

    // Intento best-effort de leer preference_id via merchant_order
    let preferenceId = null;
    try {
      const moId = payment?.order?.id || null;
      if (moId) {
        const ord = await mercadopago.merchant_orders.get(moId);
        preferenceId = ord?.body?.preference_id || null;
      }
    } catch { /* noop */ }

    const gymRef        = db.collection('gimnasios').doc(gimnasioId);
    const licenciaDatos = gymRef.collection('licencia').doc('datos');   // ⬅️ fuente de verdad
    const licenciaCfg   = gymRef.collection('licencia').doc('config');  // cache / compat
    const txIdRef       = db.collection('_mp_processed').doc(String(payment.id)); // marca idempotente
    const historialRef  = gymRef.collection('licencia').doc('historial').collection('pagos').doc(String(payment.id));
    const prefRef       = preferenceId ? gymRef.collection('licencia').doc('prefs').collection('items').doc(preferenceId) : null;

    await db.runTransaction(async (transaction) => {
      // ⛑️ idempotencia: si ya procesamos este payment, salimos
      const already = await transaction.get(txIdRef);
      if (already.exists) return;

      // === leer y normalizar plan ===
      const planObj = await readPlanById(planId);

      const duracion      = Number(planObj.duracion ?? planObj.duracionDias ?? 30);
      const montoOriginal = Number(planObj.precio ?? 0);
      const tier          = planObj.tier || 'custom';
      const maxUsuarios   = Number(planObj.maxUsuarios ?? 0);

      const modulesMap = normalizePlanModules(planObj);
      const limits     = normalizePlanLimits(planObj, maxUsuarios);

      // SIN encadenar: siempre desde ahora
      const fechaInicio = new Date();
      const fechaVencimiento = new Date(fechaInicio);
      fechaVencimiento.setDate(fechaVencimiento.getDate() + duracion);

      const montoPagado = Number(payment.transaction_amount || 0);
      const descuentoAplicado = (montoOriginal > 0)
        ? Math.round((1 - (montoPagado / montoOriginal)) * 100)
        : 0;

      // 1) licencia/datos — FUENTE DE VERDAD (schema v2 + rev++)
      transaction.set(
        licenciaDatos,
        {
          version: 2,                         // versión de ESQUEMA (fija)
          rev: FieldValue.increment(1),       // ⬆️ sube en cada cambio real de licencia
          status: 'active',
          plan: planId,
          planNombre: planObj.nombre || planId,
          fechaInicio: fechaInicio,           // nombres legacy para el cliente
          fechaVencimiento: fechaVencimiento,
          usoTrial: false,
          licenciaMaxUsuarios: maxUsuarios || limits.maxMembers || 0,
          modulosPlan: modulesMap,            // legacy
          modules: modulesMap,                // normalizado
          limits,                             // normalizado
          updatedAt: FieldValue.serverTimestamp()
        },
        { merge: true }
      );

      // 1.bis) licencia/config — CACHE/compat (clientes viejos o dashboards)
      const dataCfg = {
        status: 'active',
        plan: planId,
        planNombre: planObj.nombre || planId,
        start: fechaInicio,
        expiry: fechaVencimiento,
        updatedAt: FieldValue.serverTimestamp(),
        tier,
        limits,
        licenciaMaxUsuarios: maxUsuarios || limits.maxMembers || 0,
        modules: modulesMap
      };
      transaction.set(licenciaCfg, dataCfg, { merge: true });

      // 1.ter) CACHE escritorio: gimnasios/{gymId}/config/config
      const modulosActivados = {};
      for (const [k, v] of Object.entries(modulesMap)) if (v) modulosActivados[k] = true;

      transaction.set(gymRef.collection('config').doc('config'), {
        licenciaPlanId: planId,
        licenciaNombre: planObj.nombre || planId,
        licenciaDuracionDias: duracion,
        licenciaMaxUsuarios: maxUsuarios || limits.maxMembers || 0,
        licenciaTier: tier,
        licenciaPrecio: montoOriginal,
        modulosPlan: modulesMap,
        modulosActivados,
        limits,
        ultimaActualizacionLicencia: FieldValue.serverTimestamp()
      }, { merge: true });

      // 2) historial de pagos de licencia
      transaction.set(historialRef, {
        fecha: nowTs(),
        plan: planId,
        descuentoAplicado,
        montoPagado
      }, { merge: true });

      // 3) transacciones (idempotente por payment.id)
      transaction.set(gymRef.collection('transacciones').doc(String(payment.id)), {
        monto: montoPagado,
        fecha: nowTs(),
        metodo: payment.payment_type_id,
        descuentoAplicado,
        tipo: 'licencia',
        detalle: `Licencia ${planId} - ${payment.description || ''}`
      }, { merge: true });

      // 4) preferencia → approved (si la encontramos)
      if (prefRef) {
        transaction.set(prefRef, {
          status: 'approved',
          updatedAt: nowTs()
        }, { merge: true });
      }

      // 5) REFERIDOS — acreditar al referrer consumiendo el pending del comprador (ALINEADO + email marker)
      await applyReferralCreditInTx(transaction, {
        buyerGymId: gimnasioId,
        paymentId: String(payment.id),
        planId
      });

      // 6) marca de idempotencia global
      transaction.set(txIdRef, { processedAt: nowTs() }, { merge: true });
    });

    // === EMAIL post-transacción (no bloquea) ===
    try {
      const approved = await db.doc(`gimnasios/${gimnasioId}/referrals/applied_approved`).get();
      if (approved.exists) {
        const usedCode = approved.data()?.usedCode || null;
        const referrerGymId = approved.data()?.referrerGymId || null;
        if (referrerGymId) {
          await sendReferralCongratsEmail({
            referrerGymId,
            buyerGymId: gimnasioId,
            usedCode,
            paymentId: String(payment.id)
          });
        }
      }
    } catch (e) {
      console.warn('sendReferralCongratsEmail warn:', e?.message);
    }

    // notificación (no bloquea)
    try {
      const gymIdFromExt = (payment.external_reference || '').split('|')[0]?.split(':')[1];
      if (gymIdFromExt) {
        admin.messaging().sendToTopic(gymIdFromExt, {
          notification: { title: '🎉 ¡Licencia activada!', body: `Plan ${payment.description || ''}` }
        }).catch(()=>{});
      }
    } catch {}

    return { ok: true };
  } catch (e) {
    console.error('processLicensePaymentById error:', e);
    return { ok: false, reason: 'exception', error: e?.message };
  }
}

// ==============================
//  LICENCIAS — crear preferencia
// ==============================
app.get('/crear-link-pago', async (req, res) => {
  const { gimnasioId, plan, ref, format } = req.query;
  if (!gimnasioId || !plan) return res.status(400).send('Faltan parametros');

  try {
    // lee plan (catálogo raíz)
    const planObj = await readPlanById(String(plan));
    const precio = Number(planObj.precio || 0);

    // descuento por referidos del COMPRADOR (tope 20%) — esquema nuevo
    const pct          = await getReferralDiscountPctForBuyer(gimnasioId);
    const factor       = Math.max(0, 1 - pct / 100);
    const precioConDto = Number((precio * factor).toFixed(2));
    const discountAmt  = Math.max(0, Number((precio - precioConDto).toFixed(2)));

    const titleBase   = `Licencia ${planObj.nombre || plan}`;
    const titleConDto = pct > 0 ? `${titleBase} (−${pct}% referidos)` : titleBase;

    const preference = {
      items: [{
        title: titleConDto,
        description: pct > 0 ? `Incluye descuento por referidos de ${pct}%` : titleBase,
        unit_price: precioConDto,
        quantity: 1
      }],
      ...(pct > 0 ? { coupon_code: `REFERIDOS_${pct}`, coupon_amount: discountAmt } : {}),
      statement_descriptor: 'NICHEAS GYM',
      // external_reference conserva ref y disc por compat, pero el crédito real sale de applied_pending
      external_reference: `gym:${gimnasioId}|plan:${plan}|ref:${ref || ''}|disc:${pct}`,
      notification_url: `${process.env.PUBLIC_BASE_URL}/webhook`,
      back_urls: {
        success: `${process.env.PUBLIC_BASE_URL}/success`,
        failure: `${process.env.PUBLIC_BASE_URL}/failure`,
        pending: `${process.env.PUBLIC_BASE_URL}/pending`
      },
      auto_return: 'approved'
    };

    const result = await mercadopago.preferences.create(preference);

    // Guardamos la preferencia pendiente bajo el gym (para dashboard/UX)
    try {
      const prefRef = db.doc(`gimnasios/${gimnasioId}/licencia/prefs/items/${result.body.id}`);
      await prefRef.set({
        plan,
        status: 'pending',
        init_point: result.body.init_point,
        createdAt: nowTs(),
        updatedAt: nowTs()
      }, { merge: true });
    } catch { /* noop */ }

    if (format === 'json') {
      return res.json({
        init_point: result.body.init_point,
        sandbox_init_point: result.body.sandbox_init_point,
        preference_id: result.body.id,
        descuento_pct: pct,
        descuento_monto: discountAmt
      });
    }
    return res.redirect(302, result.body.init_point);
  } catch (e) {
    console.error('Error al generar link:', e);
    return res.status(500).send('Error interno');
  }
});

// ==============================
//  LICENCIAS — Webhook + páginas de retorno
// ==============================
app.post('/webhook', async (req, res) => {
  try {
    console.log('📩 Webhook Licencias:', JSON.stringify(req.body, null, 2));

    let paymentId = req.body?.data?.id || req.body?.id || null;
    const topic   = req.body?.topic || req.body?.type || null;

    // MP a veces manda: { topic: 'merchant_order', resource: '.../merchant_orders/{id}' }
    if (!paymentId && (topic === 'merchant_order' || req.body?.resource)) {
      const resUrl = req.body?.resource || '';
      const m = /merchant_orders\/(\d+)/.exec(resUrl);
      const moId = m ? m[1] : null;

      if (moId) {
        try {
          const ord = await mercadopago.merchant_orders.get(moId);
          const payments = ord?.body?.payments || [];
          // preferimos el aprobado
          const pay = payments.find(p => p?.status === 'approved') || payments[0] || null;
          if (pay?.id) paymentId = String(pay.id);
        } catch (e) {
          console.warn('merchant_order lookup error:', e?.message);
        }
      }
    }

    if (!paymentId) return res.status(200).send('OK'); // idempotente
    const r = await processLicensePaymentById(String(paymentId));
    console.log('webhook result:', r);
    return res.status(200).send('OK');
  } catch (error) {
    console.error('❌ Error en webhook licencias:', error);
    return res.status(200).send('OK'); // evitá reintentos agresivos de MP
  }
});

function successHtml(msg) {
  return `<!doctype html><meta charset="utf-8"><body style="font-family:sans-serif;padding:20px">
  <h2>${msg}</h2><p>Podés cerrar esta pestaña y volver a la app.</p></body>`;
}
app.get(['/success','/exito','/éxito'], async (req, res) => {
  try {
    const paymentId = req.query.payment_id || req.query.collection_id || null;
    if (paymentId) await processLicensePaymentById(String(paymentId)); // idempotente
    return res.status(200).send(successHtml('Pago aprobado ✅'));
  } catch {
    return res.status(200).send(successHtml('Pago recibido (procesando)'));
  }
});
app.get('/failure', (req, res) => res.status(200).send(successHtml('El pago no pudo completarse ❌')));
app.get('/pending', (req, res) => res.status(200).send(successHtml('Pago pendiente ⏳')));
app.get(['/','/ok','/health'], (req,res)=> res.send('OK'));

// === Ruta opcional de previsualización del email (DEV) ===
app.get('/_preview/referral-email', (req, res) => {
  const html = getReferralEmailHtml({
    referrerGymName: req.query.refName || 'Gimnasio Referidor',
    buyerGymName: req.query.buyerName || 'Gimnasio Nuevo',
    usedCode: req.query.code || 'FIT-GYM-AB12',
    paymentId: req.query.paymentId || '1234567890'
  });
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(html);
});

// ==============================
//  OAUTH MERCADO PAGO (gimnasios)
// ==============================
app.get('/mp/oauth/start', (req, res) => {
  const gymId = req.query.gymId || 'na';
  const clientId = process.env.MP_CLIENT_ID;
  const redirectUri = process.env.MP_REDIRECT_URI;
  if (!clientId || !redirectUri) return res.status(400).send('Faltan MP_CLIENT_ID/MP_REDIRECT_URI');

  const q = new URLSearchParams({
    response_type: 'code',
    client_id: clientId,
    redirect_uri: redirectUri,
    state: gymId
  });
  res.redirect(`https://auth.mercadopago.com/authorization?${q.toString()}`);
});
app.get('/oauth/start', (req, res) => {
  const url = new URL(`${req.protocol}://${req.get('host')}/mp/oauth/start`);
  if (req.query.gymId) url.searchParams.set('gymId', req.query.gymId);
  res.redirect(url.toString());
});
async function handleOauthCallback(req, res) {
  try {
    const code = req.query.code;
    const gymId = req.query.state;
    if (!code || !gymId) return res.status(400).send('Faltan code/state');

    const tokenJson = await mpOAuthTokenExchange({ code, redirectUri: process.env.MP_REDIRECT_URI });
    const accessToken = tokenJson.access_token;
    const refreshToken = tokenJson.refresh_token;
    const expiresIn = Number(tokenJson.expires_in || 0);
    const expiresAt = Date.now() + expiresIn * 1000;

    let sellerId = tokenJson.user_id || null;
    let sellerNickname = null;
    try {
      const me = await mpGetUserMe(accessToken);
      sellerId = me.id || sellerId;
      sellerNickname = me.nickname || me.nickname_ml || null;
    } catch {}

    await db.doc(`gimnasios/${gymId}/integraciones/mp`).set({
      access_token: accessToken,
      refresh_token: refreshToken,
      token_type: tokenJson.token_type || 'bearer',
      scope: tokenJson.scope || null,
      expires_at: expiresAt, // número (ms)
      seller_id: sellerId,
      seller_nickname: sellerNickname,
      updated_at: nowTs()
    }, { merge: true });

    res.status(200).send(`<html><body style="font-family:sans-serif">
      <h2>Cuenta de Mercado Pago conectada ✅</h2>
      <p>Gimnasio: ${gymId}</p>
      <p>Ya podés cerrar esta pestaña.</p>
    </body></html>`);
  } catch (e) {
    console.error('OAuth callback error:', e);
    res.status(500).send('Error en OAuth callback');
  }
}
app.get('/mp/oauth/callback', handleOauthCallback);
app.get('/oauth/callback', handleOauthCallback);

app.post('/mp/oauth/refresh/:gymId', async (req, res) => {
  try {
    const gymId = req.params.gymId;
    const docRef = db.doc(`gimnasios/${gymId}/integraciones/mp`);
    const snap = await docRef.get();
    if (!snap.exists) return res.status(404).json({ error: 'Gym sin integración' });

    const data = snap.data() || {};
    if (!data.refresh_token) return res.status(400).json({ error: 'No hay refresh_token' });

    const tokenJson = await mpOAuthRefresh(data.refresh_token);
    const accessToken = tokenJson.access_token;
    const newRefresh = tokenJson.refresh_token || data.refresh_token;
    const expiresIn = Number(tokenJson.expires_in || 0);
    const expiresAt = Date.now() + expiresIn * 1000;

    await docRef.set({
      access_token: accessToken,
      refresh_token: newRefresh,
      token_type: tokenJson.token_type || 'bearer',
      scope: tokenJson.scope || data.scope || null,
      expires_at: expiresAt,
      updated_at: nowTs()
    }, { merge: true });

    res.json({ ok: true, expires_at: expiresAt });
  } catch (e) {
    console.error('Refresh error:', e);
    res.status(500).json({ error: 'Error en refresh' });
  }
});

// ==============================
//  DISPOSITIVOS (multi-PC)
// ==============================
async function getGymLimits(gymId) {
  const cache = await db.doc(`gimnasios/${gymId}/config/config`).get();
  const limits = cache.exists ? (cache.data()?.limits || null) : null;
  if (limits) return limits;

  const lic = await db.doc(`gimnasios/${gymId}/licencia/config`).get();
  const l2 = lic.exists ? (lic.data()?.limits || null) : null;
  return l2 || { maxDevices: 1, maxBranches: 1, maxOfflineHours: 168, maxMembers: 0 };
}
async function countActiveDevices(gymId) {
  const qs = await db.collection(`gimnasios/${gymId}/devices`).where('revoked', '!=', true).get();
  return qs.size;
}

// CLAIM
app.post('/devices/claim', async (req, res) => {
  try {
    const { gimnasioId, hwid, name } = req.body || {};
    if (!gimnasioId || !hwid) return res.status(400).json({ error: 'Faltan gimnasioId/hwid' });

    const limits = await getGymLimits(gimnasioId);
    const max = Number(limits.maxDevices ?? 1);

    const devRef = db.doc(`gimnasios/${gimnasioId}/devices/${hwid}`);
    const snap = await devRef.get();

    if (!snap.exists) {
      const current = await countActiveDevices(gimnasioId);
      if (current >= max) return res.status(403).json({ error: 'Cupo de dispositivos alcanzado' });

      await devRef.set({
        hwid,
        name: name || null,
        claimedAt: nowTs(),
        lastSeenAt: nowTs(),
        revoked: false
      }, { merge: true });

      return res.json({ ok: true, claimed: true, remaining: Math.max(0, max - (current + 1)), maxDevices: max });
    }

    const data = snap.data() || {};
    if (data.revoked) return res.status(403).json({ error: 'Dispositivo revocado' });

    await devRef.set({ lastSeenAt: nowTs(), name: name || data.name || null }, { merge: true });
    const current = await countActiveDevices(gimnasioId);
    return res.json({ ok: true, claimed: false, remaining: Math.max(0, max - current), maxDevices: max });
  } catch (e) {
    console.error('devices/claim error:', e);
    res.status(500).json({ error: 'Error interno' });
  }
});

// HEARTBEAT
app.post('/devices/heartbeat', async (req, res) => {
  try {
    const { gimnasioId, hwid } = req.body || {};
    if (!gimnasioId || !hwid) return res.status(400).json({ error: 'Faltan gimnasioId/hwid' });

    const devRef = db.doc(`gimnasios/${gimnasioId}/devices/${hwid}`);
    const snap = await devRef.get();
    if (!snap.exists) return res.status(404).json({ error: 'Device no registrado' });
    if (snap.data()?.revoked) return res.status(403).json({ error: 'Revocado' });

    await devRef.set({ lastSeenAt: nowTs() }, { merge: true });
    res.json({ ok: true });
  } catch (e) {
    console.error('devices/heartbeat error:', e);
    res.status(500).json({ error: 'Error interno' });
  }
});

// REVOKE
app.post('/devices/revoke', async (req, res) => {
  try {
    const { gimnasioId, hwid } = req.body || {};
    if (!gimnasioId || !hwid) return res.status(400).json({ error: 'Faltan gimnasioId/hwid' });

    const devRef = db.doc(`gimnasios/${gimnasioId}/devices/${hwid}`);
    await devRef.set({ revoked: true, revokedAt: nowTs() }, { merge: true });
    res.json({ ok: true });
  } catch (e) {
    console.error('devices/revoke error:', e);
    res.status(500).json({ error: 'Error interno' });
  }
});

// ==============================
//  MEMBRESÍAS (altas/renovaciones) — OPCIONAL
// ==============================
app.post('/memberships/register', async (req, res) => {
  try {
    const { gimnasioId, socio } = req.body || {};
    if (!gimnasioId || !socio || !socio.nombre) {
      return res.status(400).json({ error: 'Faltan datos (gimnasioId, socio.nombre)' });
    }
    const socioId = socio.socioId || socio.dni || db.collection('_ids').doc().id;
    const ref = db.doc(`gimnasios/${gimnasioId}/socios/${socioId}`);

    await ref.set({
      socioId,
      dni: socio.dni || null,
      nombre: socio.nombre,
      email: socio.email || null,
      telefono: socio.telefono || null,
      estado: 'pendiente_pago',
      creadoEn: nowTs(),
      actualizadoEn: nowTs()
    }, { merge: true });

    res.json({ socioId });
  } catch (e) {
    console.error('register socio error:', e);
    res.status(500).json({ error: 'Error creando socio' });
  }
});

app.post('/memberships/checkout', async (req, res) => {
  try {
    const { gimnasioId, socioId, planId } = req.body || {};
    if (!gimnasioId || !socioId || !planId) {
      return res.status(400).json({ error: 'Faltan datos (gimnasioId, socioId, planId)' });
    }

    let planSnap = await db.doc(`gimnasios/${gimnasioId}/planes/${planId}`).get();
    if (!planSnap.exists) planSnap = await db.doc(`planes/${planId}`).get();
    if (!planSnap.exists) return res.status(404).json({ error: 'Plan no encontrado' });
    const plan = planSnap.data() || {};
    const precio = Number(plan.precio || plan.Precio || 0);
    if (!precio) return res.status(400).json({ error: 'Plan sin precio' });

    const gymToken = await getValidGymAccessToken(gimnasioId);

    const preference = {
      items: [{
        title: `Membresía ${plan.nombre || plan.Nombre || planId}`,
        unit_price: precio,
        quantity: 1
      }],
      external_reference: `mbr|${gimnasioId}|${socioId}|${planId}`,
      notification_url: `${process.env.PUBLIC_BASE_URL}/webhook-memberships`,
      back_urls: {
        success: `${process.env.PUBLIC_BASE_URL}/memberships/success`,
        failure: `${process.env.PUBLIC_BASE_URL}/memberships/failure`
      },
      auto_return: 'approved'
    };

    const resp = await fetch('https://api.mercadopago.com/checkout/preferences', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${gymToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(preference)
    });
    if (!resp.ok) {
      const t = await resp.text();
      throw new Error(`Crear preferencia membresía falló: ${resp.status} ${t}`);
    }
    const pref = await resp.json();
    res.json({ init_point: pref.init_point, preference_id: pref.id });
  } catch (e) {
    console.error('checkout membresía error:', e);
    res.status(500).json({ error: 'Error iniciando checkout' });
  }
});

app.post('/memberships/mark-paid-manual', async (req, res) => {
  try {
    const { gimnasioId, socioId, planId, metodo } = req.body || {};
    if (!gimnasioId || !socioId || !planId) {
      return res.status(400).json({ error: 'Faltan datos (gimnasioId, socioId, planId)' });
    }

    let planSnap = await db.doc(`gimnasios/${gimnasioId}/planes/${planId}`).get();
    if (!planSnap.exists) planSnap = await db.doc(`planes/${planId}`).get();
    if (!planSnap.exists) return res.status(404).json({ error: 'Plan no encontrado' });
    const plan = planSnap.data() || {};
    const precio = Number(plan.precio || plan.Precio || 0);

    await extendMembershipTx({
      gimnasioId, socioId, planId, plan,
      monto: precio, metodo: metodo || 'manual', paymentId: null
    });

    res.json({ ok: true });
  } catch (e) {
    console.error('mark-paid-manual membresía error:', e);
    res.status(500).json({ error: 'Error marcando pago manual' });
  }
});

app.post('/webhook-memberships', async (req, res) => {
  try {
    const paymentId = req.body?.data?.id;
    const topic = req.body?.type || req.body?.topic;
    if (!paymentId) return res.status(200).send('OK');

    // Leer external_reference (con tu token global)
    let payment = null;
    try {
      const p = await mercadopago.payment.get(paymentId);
      payment = p.body;
    } catch (e) {
      console.warn('No pude leer pago con token global:', e?.message);
      return res.status(200).send('OK');
    }

    const extRef = payment?.external_reference || '';
    if (!extRef.startsWith('mbr|')) return res.status(200).send('OK');

    const [, gymId, socioId, planId] = extRef.split('|');
    if (!gymId || !socioId || !planId) return res.status(200).send('OK');

    // Confirmar estado con token del GYM
    const gymToken = await getValidGymAccessToken(gymId);
    const payResp = await fetch(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
      headers: { Authorization: `Bearer ${gymToken}` }
    });
    if (!payResp.ok) {
      console.error('payments get (gym) error:', await payResp.text());
      return res.status(200).send('OK');
    }
    const sellerPayment = await payResp.json();
    if (sellerPayment.status !== 'approved') return res.status(200).send('OK');

    let planSnap = await db.doc(`gimnasios/${gymId}/planes/${planId}`).get();
    if (!planSnap.exists) planSnap = await db.doc(`planes/${planId}`).get();
    if (!planSnap.exists) return res.status(200).send('OK');
    const plan = planSnap.data() || {};
    const monto = Number(sellerPayment.transaction_amount || 0);

    await extendMembershipTx({
      gimnasioId: gymId,
      socioId,
      planId,
      plan,
      monto,
      metodo: sellerPayment.payment_type_id || 'mp',
      paymentId
    });

    res.status(200).send('OK');
  } catch (e) {
    console.error('webhook-memberships error:', e);
    res.status(200).send('OK');
  }
});

// ==============================
//  TIENDA (carrito / checkout / webhook) — OPCIONAL
// ==============================
app.post('/store/orders', async (req, res) => {
  try {
    const { gimnasioId, items, buyer } = req.body || {};
    if (!gimnasioId || !Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ error: 'Faltan datos (gimnasioId, items)' });
    }

    let total = 0;
    const validated = [];

    for (const it of items) {
      const { categoriaId, subcategoriaId, productId, quantity, variant } = it || {};
      if (!categoriaId || !subcategoriaId || !productId || !quantity) {
        return res.status(400).json({ error: 'Item incompleto' });
      }
      const prodRef = db.doc(`gimnasios/${gimnasioId}/tienda_categorias/${categoriaId}/subcategorias/${subcategoriaId}/productos/${productId}`);
      const snap = await prodRef.get();
      if (!snap.exists) return res.status(404).json({ error: `Producto ${productId} no encontrado` });
      const p = snap.data() || {};

      const price = (p.PrecioOferta ?? p.Precio ?? 0);
      const cantidad = Number(quantity);
      if (price <= 0 || cantidad <= 0) return res.status(400).json({ error: 'Precio/cantidad inválida' });

      validated.push({
        categoriaId, subcategoriaId, productId,
        nombre: p.Nombre || 'Producto',
        unit_price: Number(price),
        quantity: cantidad,
        variant: variant || null,
        img: p.ImgUrl || null
      });
      total += price * cantidad;
    }

    const orderRef = db.collection(`gimnasios/${gimnasioId}/tienda_ordenes`).doc();
    const order = {
      id: orderRef.id,
      gimnasioId,
      items: validated,
      buyer: buyer || null,
      total: Number(total.toFixed(2)),
      status: 'pending',
      createdAt: nowTs(),
      updatedAt: nowTs()
    };

    await orderRef.set(order);
    res.json(order);
  } catch (e) {
    console.error('create order error:', e);
    res.status(500).json({ error: 'Error creando orden' });
  }
});

app.post('/store/orders/:orderId/checkout', async (req, res) => {
  try {
    const { gimnasioId } = req.body || {};
    const { orderId } = req.params;
    if (!gimnasioId || !orderId) return res.status(400).json({ error: 'Faltan datos' });

    const orderRef = db.doc(`gimnasios/${gimnasioId}/tienda_ordenes/${orderId}`);
    const snap = await orderRef.get();
    if (!snap.exists) return res.status(404).json({ error: 'Orden no encontrada' });
    const order = snap.data();
    if (order.status !== 'pending') return res.status(400).json({ error: 'Orden no pendiente' });

    const gymToken = await getValidGymAccessToken(gimnasioId);

    const preference = {
      items: order.items.map(i => ({
        title: i.nombre,
        unit_price: i.unit_price,
        quantity: i.quantity
      })),
      external_reference: `store|${gimnasioId}|${orderId}`,
      notification_url: `${process.env.PUBLIC_BASE_URL}/webhook-store?gymId=${encodeURIComponent(gimnasioId)}&orderId=${encodeURIComponent(orderId)}`,
      back_urls: {
        success: `${process.env.PUBLIC_BASE_URL}/store/success`,
        failure: `${process.env.PUBLIC_BASE_URL}/store/failure`
      },
      auto_return: 'approved'
    };

    const resp = await fetch('https://api.mercadopago.com/checkout/preferences', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${gymToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(preference)
    });
    if (!resp.ok) {
      const t = await resp.text();
      throw new Error(`Crear preferencia (gym) falló: ${resp.status} ${t}`);
    }
    const pref = await resp.json();

    await orderRef.set({
      mp_preference_id: pref.id,
      updatedAt: nowTs()
    }, { merge: true });

    res.json({ init_point: pref.init_point, sandbox_init_point: pref.sandbox_init_point, preference_id: pref.id });
  } catch (e) {
    console.error('checkout error:', e);
    res.status(500).json({ error: 'Error iniciando checkout' });
  }
});

app.post('/store/orders/:orderId/mark-paid-manual', async (req, res) => {
  try {
    const { gimnasioId, metodo } = req.body || {};
    const { orderId } = req.params;
    if (!gimnasioId || !orderId) return res.status(400).json({ error: 'Faltan datos' });

    const orderRef = db.doc(`gimnasios/${gimnasioId}/tienda_ordenes/${orderId}`);
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(orderRef);
      if (!snap.exists) throw new Error('Orden no encontrada');
      const order = snap.data();
      if (order.status === 'paid') return; // idempotente

      for (const it of order.items || []) {
        await discountStockTx(tx, gimnasioId, it);
      }

      tx.set(orderRef, {
        status: 'paid',
        paidAt: nowTs(),
        payment_method: metodo || 'manual',
        updatedAt: nowTs()
      }, { merge: true });

      const txRef = db.collection(`gimnasios/${gimnasioId}/transacciones`).doc();
      tx.set(txRef, {
        monto: order.total,
        fecha: nowTs(),
        metodo: metodo || 'manual',
        tipo: 'venta_tienda',
        orderId
      });

      const medio = medioKeyFrom(metodo || 'manual');
      acumularIngresoDiarioTx(tx, gimnasioId, 'tienda', Number(order.total || 0), medio);
    });

    res.json({ ok: true });
  } catch (e) {
    console.error('mark-paid-manual error:', e);
    res.status(500).json({ error: 'Error marcando pago manual' });
  }
});

app.post('/webhook-store', async (req, res) => {
  try {
    const gymId = req.query.gymId;
    const orderId = req.query.orderId;
    const paymentId = req.body?.data?.id;
    const topic = req.body?.type || req.body?.topic;

    console.log('📩 Webhook Store:', { gymId, orderId, topic, paymentId });

    if (!gymId || !orderId || !paymentId) {
      return res.status(200).send('OK');
    }

    const gymToken = await getValidGymAccessToken(gymId);

    const payResp = await fetch(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
      headers: { Authorization: `Bearer ${gymToken}` }
    });
    if (!payResp.ok) {
      console.error('payments get error:', await payResp.text());
      return res.status(200).send('OK');
    }
    const payment = await payResp.json();
    if (payment.status !== 'approved') return res.status(200).send('OK');

    const orderRef = db.doc(`gimnasios/${gymId}/tienda_ordenes/${orderId}`);

    const prev = await orderRef.get();
    if (!prev.exists) return res.status(200).send('OK');
    if (prev.data().status === 'paid') return res.status(200).send('OK');

    await db.runTransaction(async (tx) => {
      const snap = await tx.get(orderRef);
      if (!snap.exists) return;
      const order = snap.data();
      if (order.status === 'paid') return;

      for (const it of order.items || []) {
        await discountStockTx(tx, gymId, it);
      }

      tx.set(orderRef, {
        status: 'paid',
        paidAt: nowTs(),
        payment_id: paymentId,
        payment_method: payment.payment_type_id || null,
        updatedAt: nowTs()
      }, { merge: true });

      const txRef = db.collection(`gimnasios/${gymId}/transacciones`).doc();
      tx.set(txRef, {
        monto: order.total,
        fecha: nowTs(),
        metodo: payment.payment_type_id || 'mp',
        tipo: 'venta_tienda',
        orderId,
        paymentId
      });

      const medio = medioKeyFrom(payment.payment_type_id || 'mp'); // MP => online
      acumularIngresoDiarioTx(tx, gymId, 'tienda', Number(order.total || 0), medio);
    });

    res.status(200).send('OK');
  } catch (e) {
    console.error('webhook-store error:', e);
    res.status(200).send('OK');
  }
});

// ==============================
//  Descuento de stock (TX)
// ==============================
async function discountStockTx(tx, gimnasioId, it) {
  const { categoriaId, subcategoriaId, productId, quantity, variant } = it;
  const pRef = db.doc(`gimnasios/${gimnasioId}/tienda_categorias/${categoriaId}/subcategorias/${subcategoriaId}/productos/${productId}`);
  const pSnap = await tx.get(pRef);
  if (!pSnap.exists) throw new Error(`Producto ${productId} no existe`);
  const p = pSnap.data() || {};

  const qty = Number(quantity || 0);
  if (!qty || qty <= 0) return;

  if (Array.isArray(p.StockPorVariante) && p.StockPorVariante.length > 0 && variant) {
    const col = normalize(variant.Color);
    const tal = normalize(variant.Talle);

    const list = [...p.StockPorVariante];
    let found = false;

    for (let i = 0; i < list.length; i++) {
      const v = list[i] || {};
      const vCol = normalize(v.Color);
      const vTal = normalize(v.Talle);

      const matchColor = col ? vCol === col : !v.Color;
      const matchTalle = tal ? vTal === tal : !v.Talle;

      if (matchColor && matchTalle) {
        const st = Number(v.Stock || 0);
        if (st < qty) throw new Error(`Stock insuficiente para variante ${v.Color || ''}/${v.Talle || ''}`);
        list[i] = { ...v, Stock: st - qty };
        found = true;
        break;
      }
    }

    if (!found) throw new Error('Variante no encontrada para descontar');
    tx.set(pRef, { StockPorVariante: list, UpdatedAt: nowTs() }, { merge: true });
    return;
  }

  const stock = Number(p.Stock || 0);
  if (stock < qty) throw new Error('Stock insuficiente');
  tx.set(pRef, { Stock: stock - qty, UpdatedAt: nowTs() }, { merge: true });
}

// ==============================
//  Helper: extender/crear membresía y contabilidad
// ==============================
async function extendMembershipTx({ gimnasioId, socioId, planId, plan, monto, metodo, paymentId }) {
  const socioRef = db.doc(`gimnasios/${gimnasioId}/socios/${socioId}`);
  const txRef   = db.collection(`gimnasios/${gimnasioId}/transacciones`).doc();

  const duracion = Number(plan.duracion || plan.duracionDias || 30);
  const planNombre = plan.nombre || plan.Nombre || planId;

  await db.runTransaction(async (tx) => {
    const socioSnap = await tx.get(socioRef);

    const hoy = new Date();
    let fechaInicio = hoy;
    let tipoIngreso = 'altas'; // si no tenía vencimiento → ALTA
    if (socioSnap.exists) {
      const d = socioSnap.data() || {};
      const v = d?.fechaVencimiento;
      const venc = v?.toDate?.() || (v ? new Date(v) : null);
      if (venc) {
        tipoIngreso = 'renovaciones';         // ya tenía plan → RENOVACIÓN
        if (venc > hoy) fechaInicio = venc;   // se encadena
      }
    }
    const fechaVenc = new Date(fechaInicio);
    fechaVenc.setDate(fechaVenc.getDate() + duracion);

    // 1) Actualizar socio
    tx.set(socioRef, {
      estado: 'activo',
      planActual: planId,
      planNombre,
      fechaInicio,
      fechaVencimiento: fechaVenc,
      ultimaRenovacion: nowTs()
    }, { merge: true });

    // 2) Registrar transacción
    tx.set(txRef, {
      tipo: 'membresia',
      socioId,
      planId,
      monto,
      metodo,
      paymentId: paymentId || null,
      fecha: nowTs()
    });

    // 3) Resumen mensual (AAAA-MM)
    const ym = new Date();
    const yyyy = ym.getFullYear();
    const mm = String(ym.getMonth() + 1).padStart(2, '0');
    const resumenRef = db.doc(`gimnasios/${gimnasioId}/resumenPagos/${yyyy}-${mm}`);

    const d = socioSnap.exists ? (socioSnap.data() || {}) : {};
    const clienteNombre = d?.nombre || d?.Nombre || 'socio';
    const clienteDni    = d?.dni || d?.Dni || null;

    const pagoItem = {
      clienteDni: clienteDni,
      clienteNombre: clienteNombre,
      empleadoDni: metodo === 'manual' ? 'CAJA' : 'MP',
      fecha: new Date().toISOString(),
      metodo: metodo || 'mp',
      monto: Number(monto || 0),
      plan: planNombre,
      registradoPor: paymentId ? 'webhook' : 'manual'
    };

    tx.set(resumenRef, { pagos: admin.firestore.FieldValue.arrayUnion(pagoItem) }, { merge: true });

    // 4) Resumen día (ALTA/RENOVACIÓN, TZ BA)
    const medio = medioKeyFrom(metodo);
    acumularIngresoDiarioTx(tx, gimnasioId, tipoIngreso, Number(monto || 0), medio);
  });
}

// ==============================
//  Arranque
// ==============================
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`🚀 Webhook activo en puerto ${PORT}`);
  console.log(`🌐 Base URL: ${process.env.PUBLIC_BASE_URL || '(definir PUBLIC_BASE_URL)'}`);
});
