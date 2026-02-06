// server.js
// ======================================================
// Webhook / Backend FitSuite Pro - LICENCIAS + MP OAuth
// Fuente de verdad: gimnasios/{gymId}/licencia/datos (nombres conservados)
// Cache:            gimnasios/{gymId}/licencia/config  y  gimnasios/{gymId}/config/config
// Referidos: índice global + consumo idempotente del pending del comprador
// IMPORTA: en licencia/datos → version: FieldValue.increment(1) (sube +1 por renovación/cambio)
// ======================================================

const express = require('express');
const mercadopago = require('mercadopago');
const admin = require('firebase-admin');
const dotenv = require('dotenv');
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
  console.warn('⚠️ Falta MP_ACCESS_TOKEN');
}
mercadopago.configure({ access_token: process.env.MP_ACCESS_TOKEN });

// ==============================
//  Helpers
// ==============================
function normalize(s) { return (s || '').toString().trim().toLowerCase(); }
function nowTs() { return FieldValue.serverTimestamp(); }
const BRAND = process.env.BRAND_NAME || 'FitSuite Pro';

const BA_TZ = 'America/Argentina/Buenos_Aires';
function dayId(date = new Date(), timeZone = BA_TZ) {
  const parts = new Intl.DateTimeFormat('en-CA', { timeZone, year:'numeric', month:'2-digit', day:'2-digit' }).formatToParts(date);
  const y = parts.find(p=>p.type==='year')?.value, m = parts.find(p=>p.type==='month')?.value, d = parts.find(p=>p.type==='day')?.value;
  return `${y}-${m}-${d}`;
}
function resumenDiaRef(gymId, d=new Date()) { return db.doc(`gimnasios/${gymId}/resumen_dias/${dayId(d, BA_TZ)}`); }
function medioKeyFrom(method) { const m=normalize(method); return (m==='efectivo'||m==='cash')?'efectivo':'online'; }

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
  tx.set(ref, {
    [`ingresos.${tipo}.cantidad`]: FieldValue.increment(1),
    [`ingresos.${tipo}.total`]: FieldValue.increment(Number(monto||0)),
    [`ingresos.${tipo}.${medio}`]: FieldValue.increment(Number(monto||0)),
    ultimaActualizacion: nowTs()
  }, { merge: true });
}

// ==============================
//  Helpers de Plan (normalización)
// ==============================
async function readPlanById(planId) {
  let planSnap = await db.collection('licencias').doc(planId).get();
  if (!planSnap.exists) planSnap = await db.collection('planesLicencia').doc(planId).get();
  if (!planSnap.exists) throw new Error('Plan no encontrado');
  const plan = planSnap.data() || {};
  return { id: planId, ...plan };
}
function normalizePlanModules(plan) {
  const out = {};
  for (const key of ['modulosPlan','modulos','modules','features']) {
    const v = plan[key];
    if (!v) continue;
    if (Array.isArray(v)) {
      for (const it of v) { const s = (it||'').toString().trim(); if (s) out[s]=true; }
    } else if (typeof v === 'object') {
      for (const [k,val] of Object.entries(v)) if (k) out[k] = !!val;
    }
  }
  if (out.reports === undefined) out.reports = false;
  return out;
}
function normalizePlanLimits(plan, fallbackMaxUsuarios = 0) {
  const lim = Object.assign({}, plan.limits || {});
  const norm = {
    maxMembers:      Number(lim.maxMembers      ?? plan.maxMembers ?? plan.maxUsuarios ?? fallbackMaxUsuarios ?? 0),
    maxDevices:      Number(lim.maxDevices      ?? plan.maxDevices ?? 1),
    maxBranches:     Number(lim.maxBranches     ?? plan.maxBranches ?? 1),
    maxOfflineHours: Number(lim.maxOfflineHours ?? plan.maxOfflineHours ?? 168)
  };
  if (!Number.isFinite(norm.maxDevices) || norm.maxDevices < 1) norm.maxDevices = 1;
  if (!Number.isFinite(norm.maxBranches) || norm.maxBranches < 1) norm.maxBranches = 1;
  if (!Number.isFinite(norm.maxOfflineHours) || norm.maxOfflineHours < 24) norm.maxOfflineHours = 24;
  if (!Number.isFinite(norm.maxMembers) || norm.maxMembers < 0) norm.maxMembers = 0;
  return norm;
}

// ==============================
//  INBOX (HTML) — utilidades
// ==============================
function escapeHtml(s){return String(s||'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'","&#39;");}
function getReferralInboxHtml({ referrerGymName, buyerGymName, usedCode }) {
  const accent='#6366F1';
  return `<!doctype html><meta charset="utf-8"><body style="margin:0;background:#0f1220;font-family:Segoe UI,Roboto,Arial,sans-serif;color:#e9eefb">
  <table width="100%" cellpadding="0" cellspacing="0" style="padding:32px 0"><tr><td align="center">
  <table width="600" cellpadding="0" cellspacing="0" style="background:#151936;border-radius:14px;box-shadow:0 10px 30px rgba(0,0,0,.35);overflow:hidden">
  <tr><td style="padding:24px 28px;background:linear-gradient(135deg, ${accent}, #8b5cf6); color:#fff">
  <h1 style="margin:0;font-size:20px;letter-spacing:.3px">${BRAND}</h1><p style="margin:6px 0 0 0;opacity:.95">Programa de referidos</p></td></tr>
  <tr><td style="padding:28px"><h2 style="margin:0 0 12px 0;font-size:22px;color:#fff">¡Nuevo referido confirmado! 🎉</h2>
  <p style="margin:0 0 16px 0;line-height:1.55;color:#cfd6ee">El gimnasio <b style="color:#fff">${escapeHtml(buyerGymName)}</b> usó tu código <b style="color:#fff">${escapeHtml(usedCode||'—')}</b>.</p>
  </td></tr></table></td></tr></table></body>`;
}
async function createReferralInboxMessage({ referrerGymId, buyerGymId, usedCode, paymentId }) {
  const refGymSnap = await db.doc(`gimnasios/${referrerGymId}`).get();
  const buyerGymSnap = await db.doc(`gimnasios/${buyerGymId}`).get();
  const refName = (refGymSnap.exists ? (refGymSnap.data()?.nombre) : null) || referrerGymId;
  const buyerName = (buyerGymSnap.exists ? (buyerGymSnap.data()?.nombre) : null) || buyerGymId;
  const html = getReferralInboxHtml({ referrerGymName: refName, buyerGymName: buyerName, usedCode: usedCode || null });

  // colección: gimnasios/{referrerGymId}/inbox/{docId}
  const inboxRef = db
    .collection('gimnasios')
    .doc(referrerGymId)
    .collection('inbox')
    .doc(`ref-${String(paymentId)}`);

  await inboxRef.set({
    type: 'referral_credit',
    source: 'referral',
    level: 'info',
    title: '🎉 Nuevo referido confirmado',
    html,
    usedCode: usedCode || null,
    buyerGymId,
    tags: ['referral', 'referido'],
    displayUrl: null,
    createdAt: nowTs(),
    unread: true
  }, { merge:true });
}

// === INBOX (licencia) ===
function getLicenseInboxHtml({ brand, planNombre, fechaInicio, fechaVencimiento, descuentoAplicado, eventType }) {
  const accent = eventType === 'license_upgraded' ? '#0ea5e9'
               : eventType === 'license_renewed'  ? '#f59e0b'
               : '#22c55e';
  const head   = eventType === 'license_upgraded' ? '¡Plan mejorado! 🔼'
               : eventType === 'license_renewed'  ? '¡Licencia renovada! 🔁'
               : '¡Tu licencia está activa! ✅';
  const fmt = (d) => {
    try {
      const date = d?.toDate?.() ? d.toDate() : (d instanceof Date ? d : new Date(d));
      return new Intl.DateTimeFormat('es-AR',{ timeZone:'America/Argentina/Buenos_Aires', year:'numeric', month:'2-digit', day:'2-digit'}).format(date);
    } catch { return '—'; }
  };
  return `<!doctype html><meta charset="utf-8">
  <body style="margin:0;background:#0f1220;font-family:Segoe UI,Roboto,Arial,sans-serif;color:#e9eefb">
    <table width="100%" cellpadding="0" cellspacing="0" style="padding:32px 0"><tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background:#151936;border-radius:14px;box-shadow:0 10px 30px rgba(0,0,0,.35);overflow:hidden">
        <tr><td style="padding:24px 28px;background:linear-gradient(135deg, ${accent}, #6366f1); color:#fff">
          <h1 style="margin:0;font-size:20px;letter-spacing:.3px">${brand}</h1>
          <p style="margin:6px 0 0 0;opacity:.95">Licencia</p>
        </td></tr>
        <tr><td style="padding:28px">
          <h2 style="margin:0 0 12px 0;font-size:22px;color:#fff">${head}</h2>
          <p style="margin:0 0 12px 0;color:#cfd6ee">Plan: <b style="color:#fff">${escapeHtml(planNombre)}</b></p>
          <p style="margin:0 0 12px 0;line-height:1.55;color:#cfd6ee">Vigencia: <b style="color:#fff">${fmt(fechaInicio)}</b> → <b style="color:#fff">${fmt(fechaVencimiento)}</b></p>
          ${Number(descuentoAplicado||0) > 0 ? `<p style="margin:0 0 12px 0;color:#9aa3c7">Incluye descuento aplicado de <b>${descuentoAplicado}%</b>.</p>` : ``}
          <p style="margin:16px 0 0 0;font-size:12px;color:#9aa3c7">Podés ver módulos activos en <b>Configuración → Licencia</b>.</p>
        </td></tr>
        <tr><td style="padding:16px 28px;background:#0f122a;color:#9aa3c7;font-size:12px">© ${new Date().getFullYear()} ${brand}. Todos los derechos reservados.</td></tr>
      </table>
    </td></tr></table>
  </body>`;
}
async function createLicenseInboxMessage({ gymId, paymentId, planNombre, fechaInicio, fechaVencimiento, descuentoAplicado, eventType }) {

  // colección: gimnasios/{gymId}/inbox/{docId}
  const inboxRef = db
    .collection('gimnasios')
    .doc(gymId)
    .collection('inbox')
    .doc(`lic-${String(paymentId)}`);

  const html = getLicenseInboxHtml({
    brand: BRAND,
    planNombre,
    fechaInicio,
    fechaVencimiento,
    descuentoAplicado,
    eventType
  });
  const title =
    eventType === 'license_upgraded' ? `🔼 Plan mejorado: ${planNombre}` :
    eventType === 'license_renewed'  ? `🔁 Licencia renovada: ${planNombre}` :
                                     `✅ Licencia activada: ${planNombre}`;
  await inboxRef.set({
    type: eventType,             // license_activated | license_renewed | license_upgraded
    source: 'license',
    level: 'info',
    title,
    html,
    planNombre,
    start: fechaInicio,
    end: fechaVencimiento,
    discountPct: Number(descuentoAplicado || 0),
    tags: ['licencia', 'license'],
    displayUrl: null,
    createdAt: nowTs(),
    unread: true
  }, { merge: true });
}

// ==============================
//  REFERIDOS (nuevo esquema)
// ==============================
async function getReferralDiscountPctForBuyer(gymId) {
  try {
    const snap = await db.doc(`gimnasios/${gymId}/referrals/config`).get();
    if (!snap.exists) return 0;
    const tier = Number(snap.data()?.discountTier ?? 0);
    return Math.max(0, Math.min(isNaN(tier)?0:tier, 20));
  } catch { return 0; }
}
async function applyReferralCreditInTx(tx, { buyerGymId, paymentId, planId }) {
  const buyerRef = db.doc(`gimnasios/${buyerGymId}`);
  const pendingRef = buyerRef.collection('referrals').doc('applied_pending');
  const pendingSnap = await tx.get(pendingRef);
  if (!pendingSnap.exists) return;
  const p = pendingSnap.data() || {};
  if ((p.status || 'pending') !== 'pending') return;

  const usedCode = p.usedCode;
  const referrerGymId = p.referrerGymId;
  if (!referrerGymId || typeof referrerGymId !== 'string') return;
  if (referrerGymId === buyerGymId) return;

  const refRoot    = db.doc(`gimnasios/${referrerGymId}`);
  const refCfgRef  = refRoot.collection('referrals').doc('config');
  const refHistRef = refRoot.collection('referrals').doc('history').collection('items').doc(String(paymentId));

  const histSnap = await tx.get(refHistRef);
  if (histSnap.exists) {
    tx.set(buyerRef.collection('referrals').doc('applied_approved'), {
      usedCode: usedCode || null, referrerGymId, approvedAt: nowTs(), paymentId: String(paymentId), planId
    }, { merge: true });
    tx.set(pendingRef, { status:'consumed', consumedAt: nowTs(), paymentId: String(paymentId) }, { merge: true });
    return;
  }

  tx.set(refCfgRef, {
    totalReferrals: FieldValue.increment(1),
    discountTier: FieldValue.increment(4),
    totalPointsEarned: FieldValue.increment(100),
    pointsAvailable:   FieldValue.increment(100),
    updatedAt: nowTs()
  }, { merge: true });

  tx.set(refHistRef, { buyerGymId, paymentId:String(paymentId), planId, at: nowTs() }, { merge:true });

  tx.set(buyerRef.collection('referrals').doc('applied_approved'), {
    usedCode: usedCode || null, referrerGymId, approvedAt: nowTs(), paymentId:String(paymentId), planId
  }, { merge: true });

  tx.set(pendingRef, { status:'consumed', consumedAt: nowTs(), paymentId:String(paymentId), referrerGymId }, { merge:true });
}

// ==============================
//  OAuth / Tokens (gimnasios)
// ==============================
async function mpOAuthTokenExchange({ code, redirectUri }) {
  const body = new URLSearchParams({
    grant_type:'authorization_code',
    client_id: process.env.MP_CLIENT_ID,
    client_secret: process.env.MP_CLIENT_SECRET,
    code, redirect_uri: redirectUri
  });
  const resp = await fetch('https://api.mercadopago.com/oauth/token',{ method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'}, body });
  if (!resp.ok) throw new Error(`OAuth exchange failed: ${resp.status} ${await resp.text()}`);
  return resp.json();
}
async function mpOAuthRefresh(refreshToken) {
  const body = new URLSearchParams({
    grant_type:'refresh_token',
    client_id: process.env.MP_CLIENT_ID,
    client_secret: process.env.MP_CLIENT_SECRET,
    refresh_token: refreshToken
  });
  const resp = await fetch('https://api.mercadopago.com/oauth/token',{ method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'}, body });
  if (!resp.ok) throw new Error(`OAuth refresh failed: ${resp.status} ${await resp.text()}`);
  return resp.json();
}
async function mpGetUserMe(accessToken) {
  const resp = await fetch('https://api.mercadopago.com/users/me',{ headers:{ Authorization:`Bearer ${accessToken}` }});
  if (!resp.ok) throw new Error(`users/me failed: ${resp.status} ${await resp.text()}`);
  return resp.json();
}
async function getValidGymAccessToken(gymId) {
  const ref = db.doc(`gimnasios/${gymId}/integraciones/mp`);
  const snap = await ref.get();
  if (!snap.exists) throw new Error('Gym sin integración MP');
  const data = snap.data() || {};
  let { access_token, refresh_token, expires_at } = data;
  const soon = Date.now() + 60*1000;
  if (!access_token || !expires_at || expires_at <= soon) {
    if (!refresh_token) throw new Error('No hay refresh_token para renovar');
    const tokenJson = await mpOAuthRefresh(refresh_token);
    access_token = tokenJson.access_token;
    refresh_token = tokenJson.refresh_token || refresh_token;
    const expiresIn = Number(tokenJson.expires_in || 0);
    expires_at = Date.now() + expiresIn*1000;
    await ref.set({ access_token, refresh_token, expires_at, token_type: tokenJson.token_type || 'bearer', scope: tokenJson.scope || data.scope || null, updated_at: nowTs() }, { merge:true });
  }
  return access_token;
}

// =======================================================
//  LICENCIAS — procesar pago (idempotente)
//  *** version += 1 en licencia/datos ***
// =======================================================
async function processLicensePaymentById(paymentId) {
  try {
    const { body: payment } = await mercadopago.payment.get(paymentId);
    if (!payment || payment.status !== 'approved') {
      return { ok:false, reason:'not_approved' };
    }

    const extRef = payment.external_reference || '';
    // external_reference: gym:{gymId}|plan:{planId}|ref:{...}|disc:{pct}
    const [gymPart, planPart] = extRef.split('|');
    const gimnasioId = gymPart?.split(':')[1];
    const planId     = planPart?.split(':')[1];
    if (!gimnasioId || !planId) return { ok:false, reason:'bad_extref' };

    let preferenceId = null;
    try {
      const moId = payment?.order?.id || null;
      if (moId) {
        const ord = await mercadopago.merchant_orders.get(moId);
        preferenceId = ord?.body?.preference_id || null;
      }
    } catch {}

    const gymRef        = db.collection('gimnasios').doc(gimnasioId);
    const licenciaDatos = gymRef.collection('licencia').doc('datos');   // FUENTE (nombres conservados)
    const licenciaCfg   = gymRef.collection('licencia').doc('config');  // caché/compat
    const txIdRef       = db.collection('_mp_processed').doc(String(payment.id));
    const historialRef  = gymRef.collection('licencia').doc('historial').collection('pagos').doc(String(payment.id));
    const prefRef       = preferenceId ? gymRef.collection('licencia').doc('prefs').collection('items').doc(preferenceId) : null;

    // Variables para INBOX (se completan dentro de la TX)
    let planNombre_forInbox = null;
    let fechaInicio_forInbox = null;
    let fechaVenc_forInbox = null;
    let descuento_forInbox = 0;
    let eventType_forInbox = 'license_activated';

    await db.runTransaction(async (transaction) => {
      // idempotencia
      const already = await transaction.get(txIdRef);
      if (already.exists) return;

      // Leer plan y normalizar
      const planObj = await readPlanById(planId);
      const duracion      = Number(planObj.duracion ?? planObj.duracionDias ?? 30);
      const montoOriginal = Number(planObj.precio ?? 0);
      const tier          = planObj.tier || 'custom';
      const maxUsuarios   = Number(planObj.maxUsuarios ?? 0);

      const modulesMap = normalizePlanModules(planObj);
      const limits     = normalizePlanLimits(planObj, maxUsuarios);

      // Fechas y expiración
      const fechaInicio = new Date();
      const expiry = new Date(fechaInicio); expiry.setDate(expiry.getDate() + duracion);
      const expiryIso = expiry.toISOString();

      // Estado previo para clasificar evento y preservar campos
      const prevSnap = await transaction.get(licenciaDatos);
      const prev = prevSnap.exists ? (prevSnap.data() || {}) : {};
      const prevPlan = prev?.plan || null;
      const prevLicenseId = typeof prev.licenseId === 'string' ? prev.licenseId : null;
      const prevGrace = Number(prev.graceHours ?? 72);

      // Clasificación evento
      let eventType = 'license_activated';
      if (prevSnap.exists && prev.status === 'active') {
        eventType = (prevPlan && prevPlan !== String(planId)) ? 'license_upgraded' : 'license_renewed';
      }

      const montoPagado = Number(payment.transaction_amount || 0);
      const descuentoAplicado = (montoOriginal > 0)
        ? Math.max(0, Math.round((1 - (montoPagado / montoOriginal)) * 100))
        : 0;

      // === 1) licencia/datos — NOMBRES CONSERVADOS + version++ ===
      transaction.set(licenciaDatos, {
        expiryUtc: expiryIso,                                 // ISO string
        graceHours: prevGrace,                                // preserva o default 72
        licenseId: prevLicenseId || `${planId}-${dayId(new Date(), 'UTC')}`,
        limits: {
          maxOfflineHours: Number(limits.maxOfflineHours || 168),
          maxUsers: Number(maxUsuarios || limits.maxMembers || 0),
        },
        modules: modulesMap,                                  // mapa booleano
        plan: String(planId),
        status: 'active',
        updatedUtc: nowTs(),                                  // Timestamp
        version: FieldValue.increment(1)                      // 🔥 +1 ante renovación/cambio
      }, { merge: true });

      // === 1.b) licencia/config — cache/compat ===
      transaction.set(licenciaCfg, {
        status: 'active',
        plan: planId,
        planNombre: planObj.nombre || planId,
        start: fechaInicio,
        expiry: expiry,
        updatedAt: nowTs(),
        tier,
        limits,
        licenciaMaxUsuarios: maxUsuarios || limits.maxMembers || 0,
        modules: modulesMap
      }, { merge: true });

      // === 1.c) config/config — cache escritorio ===
      const modulosActivados = {};
      for (const [k,v] of Object.entries(modulesMap)) if (v) modulosActivados[k]=true;

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
        ultimaActualizacionLicencia: nowTs()
      }, { merge: true });

      // 2) historial
      transaction.set(historialRef, {
        fecha: nowTs(),
        plan: planId,
        descuentoAplicado,
        montoPagado
      }, { merge: true });

      // 3) transacciones
      transaction.set(gymRef.collection('transacciones').doc(String(payment.id)), {
        monto: montoPagado,
        fecha: nowTs(),
        metodo: payment.payment_type_id,
        descuentoAplicado,
        tipo: 'licencia',
        detalle: `Licencia ${planId} - ${payment.description || ''}`
      }, { merge: true });

      // 4) preferencia aprobada (si existe)
      if (prefRef) transaction.set(prefRef, { status:'approved', updatedAt: nowTs() }, { merge: true });

      // 5) referidos
      await applyReferralCreditInTx(transaction, { buyerGymId: gimnasioId, paymentId: String(payment.id), planId });

      // 6) marca idempotente
      transaction.set(txIdRef, { processedAt: nowTs() }, { merge: true });

      // Datos para inbox (post-TX)
      planNombre_forInbox  = (planObj.nombre || planId);
      fechaInicio_forInbox = fechaInicio;
      fechaVenc_forInbox   = expiry;
      descuento_forInbox   = descuentoAplicado;
      eventType_forInbox   = eventType;
    });

    // === Mensaje IN-APP del COMPRADOR (idempotente por lic-{paymentId}) ===
    try {
      await createLicenseInboxMessage({
        gymId: gimnasioId,
        paymentId: String(payment.id),
        planNombre: planNombre_forInbox || String(planId),
        fechaInicio: fechaInicio_forInbox,
        fechaVencimiento: fechaVenc_forInbox,
        descuentoAplicado: descuento_forInbox,
        eventType: eventType_forInbox
      });
    } catch (e) {
      console.warn('createLicenseInboxMessage warn:', e?.message);
    }

    // === Mensaje IN-APP al REFERIDOR (si approved existe) ===
    try {
      const approved = await db.doc(`gimnasios/${gimnasioId}/referrals/applied_approved`).get();
      if (approved.exists) {
        const usedCode = approved.data()?.usedCode || null;
        const referrerGymId = approved.data()?.referrerGymId || null;
        if (referrerGymId) {
          await createReferralInboxMessage({
            referrerGymId,
            buyerGymId: gimnasioId,
            usedCode,
            paymentId: String(payment.id)
          });
        }
      }
    } catch (e) {
      console.warn('createReferralInboxMessage warn:', e?.message);
    }

    return { ok:true };
  } catch (e) {
    console.error('processLicensePaymentById error:', e);
    return { ok:false, reason:'exception', error:e?.message };
  }
}

// ==============================
//  Crear link de pago (respeta referidos)
// ==============================
app.get('/crear-link-pago', async (req, res) => {
  const { gimnasioId, plan, ref, format } = req.query;
  if (!gimnasioId || !plan) return res.status(400).send('Faltan parametros');
  try {
    const planObj = await readPlanById(String(plan));
    const precio = Number(planObj.precio || 0);
    const pct = await getReferralDiscountPctForBuyer(gimnasioId);
    const factor = Math.max(0, 1 - pct/100);
    const precioConDto = Number((precio * factor).toFixed(2));
    const discountAmt  = Math.max(0, Number((precio - precioConDto).toFixed(2)));

    const titleBase   = `Licencia ${planObj.nombre || plan}`;
    const titleConDto = pct > 0 ? `${titleBase} (−${pct}% referidos)` : titleBase;

    const preference = {
      items: [{ title: titleConDto, description: pct>0?`Incluye descuento por referidos de ${pct}%`:titleBase, unit_price: precioConDto, quantity:1 }],
      ...(pct>0?{ coupon_code:`REFERIDOS_${pct}`, coupon_amount:discountAmt }:{}),
      statement_descriptor: 'NICHEAS GYM',
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
    try {
      const prefRef = db.doc(`gimnasios/${gimnasioId}/licencia/prefs/items/${result.body.id}`);
      await prefRef.set({ plan, status:'pending', init_point: result.body.init_point, createdAt: nowTs(), updatedAt: nowTs() }, { merge:true });
    } catch {}
    if (format === 'json') {
      return res.json({ init_point: result.body.init_point, sandbox_init_point: result.body.sandbox_init_point, preference_id: result.body.id, descuento_pct: pct, descuento_monto: discountAmt });
    }
    return res.redirect(302, result.body.init_point);
  } catch (e) {
    console.error('Error al generar link:', e);
    return res.status(500).send('Error interno');
  }
});

// =======================================================
//  CANJE DE PUNTOS (VERSIÓN FIREBASE DIRECTO)
// =======================================================
app.post('/api/referrals/redeem', async (req, res) => {
  try {
    const { gymId, rewardId, cost } = req.body;

    if (!gymId || !rewardId || !cost) {
      return res.status(400).json({ ok: false, error: 'Faltan datos' });
    }

    const costInt = Number(cost);
    
    // Referencias del Gimnasio (Cliente)
    const gymRef = db.collection('gimnasios').doc(gymId);
    const configRef = gymRef.collection('referrals').doc('config');
    const historyRef = gymRef.collection('referrals').doc('history').collection('redemptions').doc();
    
    // Referencia a TU Buzón de Admin (Colección Global)
    // Aquí es donde tu App Dev leerá los mensajes
    const adminInboxRef = db.collection('admin_notificaciones').doc(); 

    // 1. Obtener nombre del Gym (para que sepas quién es)
    const gymSnap = await gymRef.get();
    const gymName = gymSnap.exists ? (gymSnap.data().nombre || gymId) : gymId;
    const gymPhone = gymSnap.exists ? (gymSnap.data().telefono || 'Sin datos') : '';

    // 2. Transacción (Todo o nada)
    await db.runTransaction(async (t) => {
      const doc = await t.get(configRef);
      const data = doc.exists ? doc.data() : {};
      
      const currentPoints = Number(data.pointsAvailable || 0);
      const currentRedeemed = Number(data.pointsRedeemed || 0);

      // A. Verificar saldo
      if (currentPoints < costInt) {
        throw new Error('INSUFFICIENT_FUNDS'); 
      }

      // B. Restar puntos al cliente
      t.set(configRef, {
        pointsAvailable: currentPoints - costInt,
        pointsRedeemed: currentRedeemed + costInt,
        updatedAt: FieldValue.serverTimestamp()
      }, { merge: true });

      // C. Guardar historial en el cliente
      t.set(historyRef, {
        rewardId: rewardId,
        cost: costInt,
        status: 'pending', // Queda en pendiente hasta que tú se lo entregues
        redeemedAt: FieldValue.serverTimestamp()
      });

      // D. ¡AVISARTE A TI! (Escribir en admin_notificaciones)
      // Esto es lo que tu App Dev debe leer
      t.set(adminInboxRef, {
        tipo: 'CANJE_PREMIO',
        titulo: `🎁 Nuevo Canje: ${gymName}`,
        mensaje: `El gimnasio quiere canjear: ${rewardId}`,
        datos: {
            gymId: gymId,
            gymName: gymName,
            telefono: gymPhone,
            producto: rewardId,
            costo: costInt
        },
        fecha: FieldValue.serverTimestamp(),
        leido: false,      // Para que te aparezca como "Nuevo"
        estado: 'pendiente' // pendiente -> entregado
      });
    });

    console.log(`✅ Canje registrado para ${gymName}: ${rewardId}`);
    return res.json({ ok: true });

  } catch (error) {
    console.error('❌ Error en canje:', error);
    if (error.message === 'INSUFFICIENT_FUNDS') {
      return res.status(400).json({ ok: false, error: 'Puntos insuficientes' });
    }
    return res.status(500).json({ ok: false, error: 'Error interno del servidor' });
  }
});

// ==============================
//  Webhook licencias + páginas retorno
// ==============================
app.post('/webhook', async (req, res) => {
  try {
    let paymentId = req.body?.data?.id || req.body?.id || null;
    const topic   = req.body?.topic || req.body?.type || null;

    if (!paymentId && (topic === 'merchant_order' || req.body?.resource)) {
      const resUrl = req.body?.resource || '';
      const m = /merchant_orders\/(\d+)/.exec(resUrl);
      const moId = m ? m[1] : null;
      if (moId) {
        try {
          const ord = await mercadopago.merchant_orders.get(moId);
          const payments = ord?.body?.payments || [];
          const pay = payments.find(p => p?.status === 'approved') || payments[0] || null;
          if (pay?.id) paymentId = String(pay.id);
        } catch (e) { console.warn('merchant_order lookup error:', e?.message); }
      }
    }

    if (!paymentId) return res.status(200).send('OK');
    const r = await processLicensePaymentById(String(paymentId));
    console.log('webhook result:', r);
    return res.status(200).send('OK');
  } catch (error) {
    console.error('❌ Error en webhook licencias:', error);
    return res.status(200).send('OK'); // evitar reintentos agresivos
  }
});

function successHtml(msg) { return `<!doctype html><meta charset="utf-8"><body style="font-family:sans-serif;padding:20px"><h2>${msg}</h2><p>Podés cerrar esta pestaña y volver a la app.</p></body>`; }
app.get(['/success','/exito','/éxito'], async (req, res) => {
  try {
    const paymentId = req.query.payment_id || req.query.collection_id || null;
    if (paymentId) await processLicensePaymentById(String(paymentId)); // idempotente
    return res.status(200).send(successHtml('Pago aprobado ✅'));
  } catch {
    return res.status(200).send(successHtml('Pago recibido (procesando)'));
  }
});
app.get('/failure', (req,res)=> res.status(200).send(successHtml('El pago no pudo completarse ❌')));
app.get('/pending', (req,res)=> res.status(200).send(successHtml('Pago pendiente ⏳')));
app.get(['/','/ok','/health'], (req,res)=> res.send('OK'));

// =======================================================
//  NUEVO: Rutas para OAuth desde App de Escritorio
// =======================================================

// 1. Callback donde MP nos devuelve al usuario (Browser)
app.get('/mp/oauth/callback', async (req, res) => {
  const { code, state } = req.query; // "state" traerá el gymId
  
  if (!code || !state) return res.status(400).send('Faltan datos (code/state)');
  
  try {
    // A. Canjeamos el código por el Token real
    const redirectUri = `${process.env.PUBLIC_BASE_URL}/mp/oauth/callback`;
    const tokenData = await mpOAuthTokenExchange({ code, redirectUri });

    // B. Guardamos el token en Firebase (Cloud)
    // Se guarda en: gimnasios/{gymId}/integraciones/mp
    const gymId = state; 
    await db.doc(`gimnasios/${gymId}/integraciones/mp`).set({
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token,
      public_key: tokenData.public_key,
      user_id: tokenData.user_id,
      expires_at: Date.now() + (tokenData.expires_in * 1000),
      updated_at: nowTs()
    }, { merge: true });

    // C. Mostramos mensaje de éxito al usuario
    return res.send(`
      <!doctype html>
      <body style="font-family:sans-serif;text-align:center;padding:50px;background:#f0f9ff;">
        <h1 style="color:#0284c7;">¡Conexión Exitosa!</h1>
        <p>Ya hemos guardado tus credenciales en la nube.</p>
        <p>Puedes cerrar esta ventana y volver a FitSuite Pro.</p>
        <script>window.opener=null;window.open("","_self");window.close();</script>
      </body>
    `);

  } catch (error) {
    console.error('OAuth Callback Error:', error);
    return res.status(500).send(`Error vinculando: ${error.message}`);
  }
});

// 2. Endpoint para que la App de Escritorio descargue el token (Polling)
app.get('/gimnasios/:gymId/sync-token', async (req, res) => {
  const { gymId } = req.params;
  try {
    const doc = await db.doc(`gimnasios/${gymId}/integraciones/mp`).get();
    if (!doc.exists) return res.status(404).json({ ok: false });
    
    const data = doc.data();
    // Solo devolvemos si tiene access_token
    if (!data.access_token) return res.status(404).json({ ok: false });

    return res.json({
      ok: true,
      access_token: data.access_token,
      refresh_token: data.refresh_token,
      public_key: data.public_key,
      user_id: data.user_id
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
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
  return l2 || { maxDevices:1, maxBranches:1, maxOfflineHours:168, maxMembers:0 };
}
async function countActiveDevices(gymId) {
  const qs = await db.collection(`gimnasios/${gymId}/devices`).where('revoked','!=',true).get();
  return qs.size;
}
app.post('/devices/claim', async (req,res)=>{
  try {
    const { gimnasioId, hwid, name } = req.body || {};
    if (!gimnasioId || !hwid) return res.status(400).json({ error:'Faltan gimnasioId/hwid' });
    const limits = await getGymLimits(gimnasioId);
    const max = Number(limits.maxDevices ?? 1);
    const devRef = db.doc(`gimnasios/${gimnasioId}/devices/${hwid}`);
    const snap = await devRef.get();
    if (!snap.exists) {
      const current = await countActiveDevices(gimnasioId);
      if (current >= max) return res.status(403).json({ error:'Cupo de dispositivos alcanzado' });
      await devRef.set({ hwid, name: name||null, claimedAt: nowTs(), lastSeenAt: nowTs(), revoked:false }, { merge:true });
      return res.json({ ok:true, claimed:true, remaining: Math.max(0, max-(current+1)), maxDevices:max });
    }
    const data = snap.data() || {};
    if (data.revoked) return res.status(403).json({ error:'Dispositivo revocado' });
    await devRef.set({ lastSeenAt: nowTs(), name: name || data.name || null }, { merge:true });
    const current = await countActiveDevices(gimnasioId);
    return res.json({ ok:true, claimed:false, remaining: Math.max(0, max-current), maxDevices:max });
  } catch (e) {
    console.error('devices/claim error:', e); res.status(500).json({ error:'Error interno' });
  }
});
app.post('/devices/heartbeat', async (req,res)=>{
  try {
    const { gimnasioId, hwid } = req.body || {};
    if (!gimnasioId || !hwid) return res.status(400).json({ error:'Faltan gimnasioId/hwid' });
    const devRef = db.doc(`gimnasios/${gimnasioId}/devices/${hwid}`);
    const snap = await devRef.get();
    if (!snap.exists) return res.status(404).json({ error:'Device no registrado' });
    if (snap.data()?.revoked) return res.status(403).json({ error:'Revocado' });
    await devRef.set({ lastSeenAt: nowTs() }, { merge:true });
    res.json({ ok:true });
  } catch (e) {
    console.error('devices/heartbeat error:', e); res.status(500).json({ error:'Error interno' });
  }
});
app.post('/devices/revoke', async (req,res)=>{
  try {
    const { gimnasioId, hwid } = req.body || {};
    if (!gimnasioId || !hwid) return res.status(400).json({ error:'Faltan gimnasioId/hwid' });
    const devRef = db.doc(`gimnasios/${gimnasioId}/devices/${hwid}`);
    await devRef.set({ revoked:true, revokedAt: nowTs() }, { merge:true });
    res.json({ ok:true });
  } catch (e) {
    console.error('devices/revoke error:', e); res.status(500).json({ error:'Error interno' });
  }
});

// ==============================
//  Arranque
// ==============================
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`🚀 Webhook activo en puerto ${PORT}`);
  console.log(`🌐 Base URL: ${process.env.PUBLIC_BASE_URL || '(definir PUBLIC_BASE_URL)'}`);
});


