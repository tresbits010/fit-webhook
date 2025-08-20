// server.js
const express = require('express');
const mercadopago = require('mercadopago');
const admin = require('firebase-admin');
const dotenv = require('dotenv');
dotenv.config();

// ======================
//  Firebase
// ======================
const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

// ======================
//  Mercado Pago (tu token maestro, SOLO para LICENCIAS)
// ======================
mercadopago.configure({ access_token: process.env.MP_ACCESS_TOKEN });

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true })); // MP a veces manda x-www-form-urlencoded

// ======================
//  Helpers
// ======================
const monthKey = (d = new Date()) =>
  `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;

async function getGymMpToken(gymId) {
  const snap = await db.doc(`gimnasios/${gymId}/integraciones/mp`).get();
  if (!snap.exists) return null;
  const data = snap.data() || {};
  return data.access_token || null;
}

function normalizeVariant(v) {
  if (!v) return { Color: null, Talle: null };
  const Color = v.Color ?? v.color ?? v.COLOR ?? null;
  const Talle = v.Talle ?? v.talle ?? v.SIZE ?? v.size ?? null;
  return {
    Color: typeof Color === 'string' ? Color.trim() : null,
    Talle: typeof Talle === 'string' ? Talle.trim() : null,
  };
}

function eqNoCase(a, b) {
  return (a || '').toString().trim().toLowerCase() === (b || '').toString().trim().toLowerCase();
}

async function adjustStockForProduct(gymId, item) {
  // item: { categoriaId, subcategoriaId, productId, quantity, variant? }
  const { categoriaId, subcategoriaId, productId, quantity, variant } = item;
  const ref = db.doc(
    `gimnasios/${gymId}/tienda_categorias/${categoriaId}/subcategorias/${subcategoriaId}/productos/${productId}`
  );
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    if (!snap.exists) return;
    const doc = snap.data() || {};
    const q = quantity || 1;

    // Si hay variantes, actualizamos StockPorVariante
    if (Array.isArray(doc.StockPorVariante) && doc.StockPorVariante.length > 0) {
      const { Color, Talle } = normalizeVariant(variant);
      const list = doc.StockPorVariante.map((x) => ({ ...x })); // copia
      // buscamos coincidencia
      const idx = list.findIndex((x) =>
        eqNoCase(x.Color || null, Color) && eqNoCase(x.Talle || null, Talle)
      );
      if (idx >= 0) {
        const cur = parseInt(list[idx].Stock || 0, 10);
        list[idx].Stock = Math.max(0, cur - q);
        tx.update(ref, { StockPorVariante: list, UpdatedAt: admin.firestore.FieldValue.serverTimestamp() });
      } else {
        // si no encontramos combinación, no hacemos nada (o podrías registrar un warning)
      }
    } else {
      // stock simple
      const cur = parseInt(doc.Stock || 0, 10);
      tx.update(ref, {
        Stock: Math.max(0, cur - (q || 1)),
        UpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
    }
  });
}

function computeTotals(items) {
  let subtotal = 0;
  for (const it of items || []) {
    const qty = Number(it.quantity || 1);
    const price = Number(it.unit_price || it.price || 0);
    subtotal += qty * price;
  }
  return { subtotal, total: subtotal }; // extensible a envío, descuentos, etc.
}

// ======================
//  LICENCIAS (tu flujo original)
// ======================

async function handleLicenseWebhook(req, res) {
  try {
    console.log('📩 Webhook LICENCIAS recibido:', JSON.stringify(req.body, null, 2));

    const paymentId = req.body?.data?.id;
    if (!paymentId) return res.status(400).send('Sin ID de pago');

    // IMPORTANTE: Para licencias usamos tu token maestro ya configurado en mercadopago.configure
    const payment = await mercadopago.payment.get(paymentId);
    const p = payment.body;
    if (p.status !== 'approved') return res.status(200).send('Pago no aprobado');

    const extRef = p.external_reference;
    if (!extRef) return res.status(400).send('Falta external_reference');

    // Tu formato original: gym:{id}|plan:{plan}|ref:{ref?}
    const [gymPart, planPart, refPart] = extRef.split('|');
    const gimnasioId = gymPart.split(':')[1];
    const planId = planPart.split(':')[1];
    const referidoDe = refPart?.split(':')[1] || null;

    console.log('🏋️ GYM:', gimnasioId, 'PLAN:', planId, 'REF:', referidoDe);

    const gymRef = db.collection('gimnasios').doc(gimnasioId);
    const licenciaRef = gymRef.collection('licencia').doc('datos');

    await db.runTransaction(async (transaction) => {
      const planSnap = await db.collection('planesLicencia').doc(planId).get();
      if (!planSnap.exists) throw new Error('Plan no encontrado');

      const plan = planSnap.data();
      const duracion = plan.duracion || 30;
      const montoOriginal = plan.precio || 0;
      const fechaActual = new Date();

      const licenciaSnap = await transaction.get(licenciaRef);
      let fechaInicio = fechaActual;

      if (licenciaSnap.exists) {
        const vRaw = licenciaSnap.data().fechaVencimiento;
        const vencimiento =
          vRaw?.toDate?.() ? vRaw.toDate() : (vRaw ? new Date(vRaw) : null);
        if (vencimiento && vencimiento > fechaActual) fechaInicio = vencimiento;
      }

      const fechaVencimiento = new Date(fechaInicio);
      fechaVencimiento.setDate(fechaVencimiento.getDate() + duracion);

      const montoPagado = p.transaction_amount;
      const descuentoAplicado = Math.round((1 - (montoPagado / montoOriginal)) * 100);

      transaction.set(
        licenciaRef,
        {
          estado: 'activa',
          plan: planId,
          planNombre: plan.nombre,
          fechaInicio,
          fechaVencimiento,
          ultimaActualizacion: admin.firestore.FieldValue.serverTimestamp(),
          usoTrial: false,
        },
        { merge: true }
      );

      transaction.set(gymRef.collection('transacciones').doc(String(p.id)), {
        tipo: 'license',
        monto: montoPagado,
        moneda: p.currency_id,
        fecha: admin.firestore.FieldValue.serverTimestamp(),
        metodo: p.payment_type_id,
        referidoDe,
        descuentoAplicado,
        detalle: `Licencia ${planId} - ${p.description || ''}`,
      });

      transaction.set(gymRef.collection('licenciaHistorial').doc(), {
        fecha: admin.firestore.FieldValue.serverTimestamp(),
        plan: planId,
        referidoDe,
        descuentoAplicado,
        montoPagado,
      });

      transaction.set(
        gymRef.collection('resumenPagos').doc(monthKey()),
        {
          licenciasCount: admin.firestore.FieldValue.increment(1),
          licenciasMonto: admin.firestore.FieldValue.increment(montoPagado),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      if (referidoDe) {
        const refDoc = db.collection('referidos').doc(referidoDe);
        transaction.set(
          refDoc,
          { descuentoAcumulado: admin.firestore.FieldValue.increment(descuentoAplicado) },
          { merge: true }
        );
      }
    });

    await admin.messaging().sendToTopic(gimnasioId, {
      notification: {
        title: '🎉 ¡Licencia Renovada!',
        body: `Plan activo hasta el ${new Date().toLocaleDateString()}`,
      },
    });

    res.status(200).send('OK');
  } catch (error) {
    console.error('❌ Error en webhook licencias:', error);
    res.status(500).send('Error procesando pago');
  }
}

// Tu endpoint original de licencias (lo dejamos igual)
app.post('/webhook', handleLicenseWebhook);

// ======================
//  TIENDA (carrito + pagos OAuth por gym + pago manual)
// ======================

/**
 * Crea orden de tienda (pendiente).
 * body: { gimnasioId, items: [{ productId, nombre, unit_price, quantity, categoriaId, subcategoriaId, variant? }], buyer?, notes? }
 * return: { orderId }
 */
app.post('/store/orders', async (req, res) => {
  try {
    const { gimnasioId, items = [], buyer = null, notes = null } = req.body || {};
    if (!gimnasioId || !Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ error: 'gimnasioId e items son requeridos' });
    }

    const totals = computeTotals(items);
    const ordersCol = db.collection(`gimnasios/${gimnasioId}/orders`);
    const docRef = ordersCol.doc();

    const payload = {
      estado: 'pending',
      items,
      totals,
      buyer,
      notes,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    await docRef.set(payload);
    res.json({ orderId: docRef.id });
  } catch (err) {
    console.error('Error creando orden:', err);
    res.status(500).json({ error: 'Error interno' });
  }
});

/**
 * Checkout de orden: crea preferencia MP usando el token OAuth del gym.
 * path: /store/orders/:orderId/checkout
 * body: { gimnasioId, back_urls? }
 * return: { init_point }
 */
app.post('/store/orders/:orderId/checkout', async (req, res) => {
  try {
    const { orderId } = req.params;
    const { gimnasioId, back_urls } = req.body || {};
    if (!gimnasioId || !orderId) return res.status(400).json({ error: 'Faltan parámetros' });

    // Traemos la orden
    const orderRef = db.doc(`gimnasios/${gimnasioId}/orders/${orderId}`);
    const orderSnap = await orderRef.get();
    if (!orderSnap.exists) return res.status(404).json({ error: 'Orden no encontrada' });
    const order = orderSnap.data();

    if (order.estado !== 'pending')
      return res.status(409).json({ error: `Orden en estado ${order.estado}` });

    // Token OAuth de este gym
    const gymToken = await getGymMpToken(gimnasioId);
    if (!gymToken) return res.status(400).json({ error: 'Gym sin cuenta de MP conectada (OAuth)' });

    // Items MP
    const mpItems = (order.items || []).map((it) => ({
      id: it.productId,
      title: it.nombre || 'Artículo',
      quantity: Number(it.quantity || 1),
      unit_price: Number(it.unit_price || it.price || 0),
      currency_id: 'ARS',
    }));

    // Armamos preference
    const preference = {
      items: mpItems,
      external_reference: `store|gym:${gimnasioId}|order:${orderId}`,
      back_urls: back_urls || {
        success: process.env.BACK_URL_SUCCESS || 'https://example.com/success',
        failure: process.env.BACK_URL_FAILURE || 'https://example.com/failure',
      },
      auto_return: 'approved',
      // MUY IMPORTANTE: agregamos notification_url con gym y order para identificar SIN leer el pago primero
      notification_url: `${process.env.PUBLIC_BASE_URL || 'https://fit-webhook.onrender.com'}/webhook-store?gymId=${encodeURIComponent(
        gimnasioId
      )}&orderId=${encodeURIComponent(orderId)}`,
    };

    // Creamos preference con el token del GYM
    const prefResp = await mercadopago.preferences.create(preference, {
      access_token: gymToken,
    });

    // Guardamos referencia en la orden
    await orderRef.set(
      {
        mpPreferenceId: prefResp.body.id,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    res.json({ init_point: prefResp.body.init_point, preference_id: prefResp.body.id });
  } catch (err) {
    console.error('Error checkout orden:', err);
    res.status(500).json({ error: 'Error interno' });
  }
});

/**
 * Pago MANUAL: marca la orden como pagada (ej: cobro en mostrador),
 * descuenta stock y crea transacción. No usa MP.
 */
app.post('/store/orders/:orderId/mark-paid-manual', async (req, res) => {
  try {
    const { orderId } = req.params;
    const { gimnasioId, metodo = 'efectivo', referencia = 'pago-manual' } = req.body || {};
    if (!gimnasioId || !orderId) return res.status(400).json({ error: 'Faltan parámetros' });

    const orderRef = db.doc(`gimnasios/${gimnasioId}/orders/${orderId}`);
    const orderSnap = await orderRef.get();
    if (!orderSnap.exists) return res.status(404).json({ error: 'Orden no encontrada' });
    const order = orderSnap.data();

    if (order.estado === 'paid') return res.status(200).json({ ok: true, already: true });

    // Descontar stock por ítem
    for (const it of order.items || []) {
      await adjustStockForProduct(gimnasioId, it);
    }

    // Marcar como pagada y registrar transacción
    await db.runTransaction(async (tx) => {
      tx.update(orderRef, {
        estado: 'paid',
        paidAt: admin.firestore.FieldValue.serverTimestamp(),
        metodoPago: metodo,
        referenciaPago: referencia,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });

      const gymRef = db.collection('gimnasios').doc(gimnasioId);
      tx.set(gymRef.collection('transacciones').doc(`order-${orderId}`), {
        tipo: 'store',
        orderId,
        monto: order.totals?.total || 0,
        moneda: 'ARS',
        metodo,
        fecha: admin.firestore.FieldValue.serverTimestamp(),
        detalle: 'Pago manual en tienda',
      });

      tx.set(
        gymRef.collection('resumenPagos').doc(monthKey()),
        {
          tiendaCount: admin.firestore.FieldValue.increment(1),
          tiendaMonto: admin.firestore.FieldValue.increment(order.totals?.total || 0),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    });

    res.json({ ok: true });
  } catch (err) {
    console.error('Error pago manual:', err);
    res.status(500).json({ error: 'Error interno' });
  }
});

/**
 * WEBHOOK de TIENDA (OAuth): MP notificará a esta URL con ?gymId=&orderId=
 * Body: { data: { id: <paymentId> } }
 */
app.post('/webhook-store', async (req, res) => {
  const gymId = req.query.gymId;
  const orderId = req.query.orderId;

  try {
    console.log('📦 Webhook STORE:', { gymId, orderId, body: req.body });

    if (!gymId || !orderId) return res.status(400).send('Faltan gymId/orderId');

    const paymentId = req.body?.data?.id;
    if (!paymentId) return res.status(400).send('Sin ID de pago');

    // Token del gym (OAuth)
    const gymToken = await getGymMpToken(gymId);
    if (!gymToken) return res.status(200).send('Gym sin OAuth, ignorando');

    // Consultamos pago con token del gym
    const payment = await mercadopago.payment.get(paymentId, { access_token: gymToken });
    const p = payment.body;

    if (p.status !== 'approved') {
      // Podés manejar "in_process" o "rejected" si querés
      return res.status(200).send('Pago no aprobado');
    }

    // Confirmamos external_reference
    const extRef = p.external_reference || '';
    if (!extRef.includes(`order:${orderId}`)) {
      // si no coincide, igual seguimos usando orderId de query para no depender 100% de extRef
      console.warn('external_reference no coincide con orderId, seguimos por query.');
    }

    const orderRef = db.doc(`gimnasios/${gymId}/orders/${orderId}`);
    const orderSnap = await orderRef.get();
    if (!orderSnap.exists) return res.status(404).send('Orden no encontrada');
    const order = orderSnap.data();

    // Si ya está pagada, respondemos OK
    if (order.estado === 'paid') return res.status(200).send('OK');

    // Descontar stock por ítem
    for (const it of order.items || []) {
      await adjustStockForProduct(gymId, it);
    }

    // Marcar como pagada + transacción + resumen
    await db.runTransaction(async (tx) => {
      tx.update(orderRef, {
        estado: 'paid',
        paidAt: admin.firestore.FieldValue.serverTimestamp(),
        mpPaymentId: p.id,
        metodoPago: p.payment_type_id,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });

      const gymRef = db.collection('gimnasios').doc(gymId);
      tx.set(gymRef.collection('transacciones').doc(String(p.id)), {
        tipo: 'store',
        orderId,
        monto: order.totals?.total || p.transaction_amount || 0,
        moneda: p.currency_id || 'ARS',
        metodo: p.payment_type_id,
        fecha: admin.firestore.FieldValue.serverTimestamp(),
        detalle: 'Compra en tienda (MP)',
      });

      tx.set(
        gymRef.collection('resumenPagos').doc(monthKey()),
        {
          tiendaCount: admin.firestore.FieldValue.increment(1),
          tiendaMonto: admin.firestore.FieldValue.increment(order.totals?.total || p.transaction_amount || 0),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    });

    res.status(200).send('OK');
  } catch (err) {
    console.error('❌ Error webhook store:', err);
    res.status(500).send('Error');
  }
});

// ======================
//  Links para LICENCIA (tu flujo original, intacto)
// ======================
app.get('/crear-link-pago', async (req, res) => {
  const { gimnasioId, plan, ref } = req.query;
  if (!gimnasioId || !plan) return res.status(400).send('Faltan parametros');

  try {
    const planDoc = await db.collection('planesLicencia').doc(plan).get();
    if (!planDoc.exists) return res.status(404).send('Plan no encontrado');

    const datos = planDoc.data();
    const preference = {
      items: [
        {
          title: `Licencia ${datos.nombre}`,
          unit_price: datos.precio,
          quantity: 1,
        },
      ],
      external_reference: `gym:${gimnasioId}|plan:${plan}|ref:${ref || ''}`,
      back_urls: {
        success: 'https://fit-webhook.onrender.com/success',
        failure: 'https://fit-webhook.onrender.com/failure',
      },
      auto_return: 'approved',
      // Podrías también poner notification_url específico, pero tu /webhook ya funciona
    };

    const result = await mercadopago.preferences.create(preference);
    return res.send(result.body.init_point);
  } catch (e) {
    console.error('Error al generar link:', e);
    return res.status(500).send('Error interno');
  }
});

// ======================
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`🚀 Webhook activo en puerto ${PORT}`));
