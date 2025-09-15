// server.js
const express = require('express');
const mercadopago = require('mercadopago'); // para LICENCIAS (tu cuenta)
const admin = require('firebase-admin');
const dotenv = require('dotenv');
dotenv.config();

const app = express();
app.use(express.json());

// ==============================
//  Firebase Admin
// ==============================
const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

// ==============================
//  MP SDK (para LICENCIAS)
// ==============================
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

// ⬇️⬇️ PEGA EL HELPER ACÁ ⬇️⬇️
function extractPlanModules(plan = {}) {
  const out = new Set();
  const candidates = ['modulosPlan', 'modulos', 'modules', 'features'];

  for (const key of candidates) {
    const v = plan[key];
    if (!v) continue;

    if (Array.isArray(v)) {
      for (const item of v) {
        const s = (item || '').toString().trim();
        if (s) out.add(s);
      }
    } else if (typeof v === 'object') {
      for (const [k, val] of Object.entries(v)) {
        if (!k) continue;
        const enabled = (typeof val === 'boolean') ? val : !!val;
        if (enabled) out.add(k);
      }
    }
  }
  return Array.from(out);
}
// ⬆️⬆️ FIN DEL HELPER ⬆️⬆️

async function getGymIntegration(gymId) {
  const snap = await db.doc(`gimnasios/${gymId}/integraciones/mp`).get();
  return snap.exists ? snap.data() : null;
}

// --- OAuth helpers (fetch nativo Node 18+) ---
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

// ==============================
//  REFERIDOS — helpers
// ==============================

// % de descuento vigente del COMPRADOR por sus referidos (tope 20%)
async function getReferralDiscountPctForBuyer(gymId) {
  try {
    const snap = await db.collection('referidos').doc(gymId).get();
    const data = snap.exists ? (snap.data() || {}) : {};
    const usos = Number(data.usosValidos || data.usos || 0);
    const pct = Math.max(0, Math.min(usos, 20)); // TOPE 20%
    return pct;
  } catch {
    return 0;
  }
}

// ==============================
//  LICENCIAS (tu flujo actual)
// ==============================

// Crear link de pago de licencia (usa TU cuenta MP)
// AHORA: aplica descuento por referidos del COMPRADOR y REDIRIGE al checkout.
app.get('/crear-link-pago', async (req, res) => {
  const { gimnasioId, plan, ref, format } = req.query;
  if (!gimnasioId || !plan) return res.status(400).send('Faltan parametros');

  try {
    const planDoc = await db.collection('planesLicencia').doc(plan).get();
    if (!planDoc.exists) return res.status(404).send('Plan no encontrado');

    const datos = planDoc.data() || {};
    const precio = Number(datos.precio || 0);

    // 👇 descuento por referidos del comprador (tope 20%)
    const pct = await getReferralDiscountPctForBuyer(gimnasioId);
    const factor = Math.max(0, 1 - pct / 100);
    const precioConDto = Number((precio * factor).toFixed(2));

    const preference = {
      items: [{
        title: `Licencia ${datos.nombre}`,
        unit_price: precioConDto,
        quantity: 1
      }],
      metadata: {
        gimnasioId,
        plan,
        ref: ref || null,
        descuento_pct: pct,
        precio_original: precio
      },
      external_reference: `gym:${gimnasioId}|plan:${plan}|ref:${ref || ''}|disc:${pct}`,
      back_urls: {
        success: `${process.env.PUBLIC_BASE_URL}/success`,
        failure: `${process.env.PUBLIC_BASE_URL}/failure`
      },
      auto_return: 'approved'
      // (si tu cuenta MP ya tiene el webhook configurado, no hace falta notification_url aquí)
    };

    const result = await mercadopago.preferences.create(preference);

    // ➡️ Por defecto REDIRIGIMOS al init_point (mejor UX para el dueño del gym)
    if (format === 'json') {
      return res.json({ init_point: result.body.init_point, sandbox_init_point: result.body.sandbox_init_point, preference_id: result.body.id, descuento_pct: pct });
    }
    return res.redirect(302, result.body.init_point);
  } catch (e) {
    console.error('Error al generar link:', e);
    return res.status(500).send('Error interno');
  }
});

// Webhook de licencias (usa TU token global)
app.post('/webhook', async (req, res) => {
  try {
    console.log('📩 Webhook Licencias:', JSON.stringify(req.body, null, 2));
    const paymentId = req.body?.data?.id;
    if (!paymentId) return res.status(400).send('Sin ID de pago');

    const payment = await mercadopago.payment.get(paymentId);
    if (payment.body.status !== 'approved') return res.status(200).send('Pago no aprobado');

    const extRef = payment.body.external_reference;
    if (!extRef) return res.status(400).send('Falta external_reference');

    // gym:xxx|plan:yyy|ref:zzz|disc:pct
    const [gymPart, planPart, refPart, discPart] = extRef.split('|');
    const gimnasioId = gymPart.split(':')[1];
    const planId = planPart.split(':')[1];
    const referidoDe = refPart?.split(':')[1] || null;
    const discPctFromRef = Number((discPart || '').split(':')[1] || 0) || 0;

    const gymRef = db.collection('gimnasios').doc(gimnasioId);
    const licenciaRef = gymRef.collection('licencia').doc('datos');
    const configRef = db.doc(`gimnasios/${gimnasioId}/config`); // ← documento (no subcolección)

    await db.runTransaction(async (transaction) => {
      const planSnap = await db.collection('planesLicencia').doc(planId).get();
      if (!planSnap.exists) throw new Error('Plan no encontrado');

      const plan = planSnap.data() || {};
      const duracion = Number(plan.duracion || 30);
      const montoOriginal = Number(plan.precio || 0);
      const tier = plan.tier || 'custom';

      // Soporte de módulos (cualquiera de las dos claves)
      const modulosPlan = (plan.modulosPlan && typeof plan.modulosPlan === 'object')
        ? plan.modulosPlan
        : ((plan.modulos && typeof plan.modulos === 'object') ? plan.modulos : null);

      // Tope de usuarios (0 = ilimitado)
      const maxUsuarios = Number(plan.maxUsuarios || 0);

      const fechaActual = new Date();
      const licenciaSnap = await transaction.get(licenciaRef);

      let fechaInicio = fechaActual;
      if (licenciaSnap.exists) {
        const v = licenciaSnap.data().fechaVencimiento;
        const vencimiento = v?.toDate?.() || new Date(v);
        if (vencimiento && vencimiento > fechaActual) fechaInicio = vencimiento;
      }
      const fechaVencimiento = new Date(fechaInicio);
      fechaVencimiento.setDate(fechaVencimiento.getDate() + duracion);

      const montoPagado = Number(payment.body.transaction_amount || 0);
      const descuentoAplicado = (montoOriginal > 0)
        ? Math.round((1 - (montoPagado / montoOriginal)) * 100)
        : 0;

      // 1) Estado de licencia (fuente de verdad para “✓ CONTRATADO”)
      const dataLic = {
        estado: 'activa',
        plan: planId,
        planNombre: plan.nombre,
        fechaInicio,
        fechaVencimiento,
        ultimaActualizacion: FieldValue.serverTimestamp(),
        usoTrial: false
      };
      if (!Number.isNaN(maxUsuarios)) dataLic.licenciaMaxUsuarios = maxUsuarios;
      if (modulosPlan) dataLic.modulosPlan = modulosPlan;

      transaction.set(licenciaRef, dataLic, { merge: true });

      // 2) Config del gym (cache para apps)
      const dataCfg = {
        licenciaPlanId: planId,
        licenciaNombre: plan.nombre,
        licenciaTier: tier,
        licenciaPrecio: montoOriginal,
        licenciaDuracionDias: duracion,
        licenciaMaxUsuarios: maxUsuarios,
        updatedAt: FieldValue.serverTimestamp()
      };
      if (modulosPlan) dataCfg.modulosPlan = modulosPlan;

      // si viene ref y el gym NO tenía referidoDe, lo dejamos fijo
      if (referidoDe) {
        const cfgSnap = await transaction.get(configRef);
        const cfgData = cfgSnap.exists ? (cfgSnap.data() || {}) : {};
        if (!cfgData.referidoDe) dataCfg.referidoDe = referidoDe;
      }

      transaction.set(configRef, dataCfg, { merge: true });

      // 3) Registro contable
      transaction.set(gymRef.collection('transacciones').doc(paymentId), {
        monto: montoPagado,
        fecha: nowTs(),
        metodo: payment.body.payment_type_id,
        referidoDe,
        descuentoAplicado,
        tipo: 'licencia',
        detalle: `Licencia ${planId} - ${payment.body.description || ''}`
      });

      // 4) Historial de licencias
      transaction.set(gymRef.collection('licenciaHistorial').doc(), {
        fecha: nowTs(),
        plan: planId,
        referidoDe,
        descuentoAplicado,
        montoPagado
      });

      // 5) Referidos — crédito ONE-TIME al referidor (solo primera compra de este gym)
      if (referidoDe) {
        const refRoot = db.collection('referidos').doc(referidoDe);
        const creditRef = refRoot.collection('creditos').doc(gimnasioId);
        const creditSnap = await transaction.get(creditRef);

        if (!creditSnap.exists) {
          // suma +1 uso válido
          transaction.set(refRoot, { usosValidos: FieldValue.increment(1) }, { merge: true });
          // (opcional) acumular % aplicado por sus referidos
          transaction.set(refRoot, { descuentoAcumulado: FieldValue.increment(descuentoAplicado) }, { merge: true });

          transaction.set(creditRef, {
            gymReferidoId: gimnasioId,
            firstPaymentId: paymentId,
            plan: planId,
            createdAt: nowTs()
          });
        }
      }
    });

    // 🔔 Notificación push (fire-and-forget, sin await)
    admin.messaging().sendToTopic(gimnasioId, {
      notification: {
        title: '🎉 ¡Licencia Renovada!',
        body: `Plan ${planId} activado correctamente`
      }
    }).catch((err) => {
      console.warn('FCM error:', err?.message);
    });

    return res.status(200).send('OK');
  } catch (error) {
    console.error('❌ Error en webhook licencias:', error);
    return res.status(500).send('Error procesando pago');
  }
});


// ==============================
//  OAUTH MERCADO PAGO (gimnasios)
// ==============================

// Iniciar OAuth (ruta “oficial”)
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

// Alias para compatibilidad: /oauth/start → /mp/oauth/start
app.get('/oauth/start', (req, res) => {
  const url = new URL(`${req.protocol}://${req.get('host')}/mp/oauth/start`);
  if (req.query.gymId) url.searchParams.set('gymId', req.query.gymId);
  res.redirect(url.toString());
});

// Callback OAuth — guarda tokens del gym
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
// Alias para compatibilidad: /oauth/callback
app.get('/oauth/callback', handleOauthCallback);

// Refresh manual (opcional)
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
//  MEMBRESÍAS (altas / renovaciones)
// ==============================

// Alta de socio “pendiente_pago” (para nuevos desde la app)
app.post('/memberships/register', async (req, res) => {
  try {
    const { gimnasioId, socio } = req.body || {};
    if (!gimnasioId || !socio || !socio.nombre) {
      return res.status(400).json({ error: 'Faltan datos (gimnasioId, socio.nombre)' });
    }
    // socioId: explícito, DNI o autogenerado
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

// Inicia checkout de membresía (usa token del GYM via OAuth)
app.post('/memberships/checkout', async (req, res) => {
  try {
    const { gimnasioId, socioId, planId } = req.body || {};
    if (!gimnasioId || !socioId || !planId) {
      return res.status(400).json({ error: 'Faltan datos (gimnasioId, socioId, planId)' });
    }

    // Plan del gym o global
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

// Pago manual de membresía (caja física)
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

// Webhook de MEMBRESÍAS (MP llama con token del vendedor/gym)
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
    // Formato: mbr|{gymId}|{socioId}|{planId}
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

    // Plan
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
    res.status(200).send('OK'); // evitar reintentos agresivos
  }
});

// ==============================
//  TIENDA (carrito / checkout / webhook)
// ==============================

// Crea orden (server valida precios y disponibilidad, NO descuenta aún)
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

// Checkout con MP usando el token del GYM (OAuth)
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

// Pago manual (efectivo/transferencia en local)
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

      // === ACUMULAR RESUMEN DÍA (TIENDA, con TZ BA) ===
      const medio = medioKeyFrom(metodo || 'manual');
      acumularIngresoDiarioTx(tx, gimnasioId, 'tienda', Number(order.total || 0), medio);
    });

    res.json({ ok: true });
  } catch (e) {
    console.error('mark-paid-manual error:', e);
    res.status(500).json({ error: 'Error marcando pago manual' });
  }
});

// Webhook de TIENDA (llamado por MP con token del GYM)
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

      // === ACUMULAR RESUMEN DÍA (TIENDA, con TZ BA) ===
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

    // 3) Resumen mensual (AAAA-MM) — tal cual lo tenías
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

    // 4) === ACUMULAR RESUMEN DÍA (ALTA/RENOVACIÓN, TZ BA) ===
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
