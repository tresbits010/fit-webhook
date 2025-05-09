// 📦 fit-webhook ajustado a tu proyecto FitSuite Pro
const express = require('express');
const admin = require('firebase-admin');
const mercadopago = require('mercadopago');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');

// 🔐 Inicialización
const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

mercadopago.configure({ access_token: process.env.MP_ACCESS_TOKEN });

const app = express();
app.use(express.json());

// 🔗 1. Generar link de pago para licencia con referidos
app.get('/generar-link', async (req, res) => {
  const { gimnasioId, planId, ref } = req.query;

  if (!gimnasioId || !planId) return res.status(400).send('Faltan parámetros');

  try {
    const planSnap = await db.collection('planesLicencia').doc(planId).get();
    if (!planSnap.exists) return res.status(404).send('Plan no encontrado');

    const plan = planSnap.data();
    let precio = plan.precio;
    let descuento = 0;

    if (ref) {
      const refSnap = await db.collection('referidos').doc(ref).get();
      if (refSnap.exists) {
        descuento = Math.min((refSnap.data().usos || 0) * 2, 30);
        await db.collection('referidos').doc(ref).update({
          usos: admin.firestore.FieldValue.increment(1),
          ultimoUso: new Date().toISOString(),
        });
      }
    }

    const configSnap = await db.collection('gimnasios').doc(gimnasioId).collection('configuracionMercadoPago').doc('datos').get();
    if (!configSnap.exists) return res.status(403).send('Config MP no encontrada');

    const accessToken = configSnap.data().accessToken;
    const precioFinal = precio * (1 - descuento / 100);

    const preferencia = {
      items: [
        {
          title: `Licencia ${plan.nombre} (${plan.duracion} días)` + (descuento > 0 ? ` -${descuento}%` : ''),
          quantity: 1,
          currency_id: 'ARS',
          unit_price: precioFinal,
        },
      ],
      external_reference: `tipo:licencia;gim:${gimnasioId};plan:${plan.nombre}` + (ref ? `;ref:${ref}` : ''),
      notification_url: process.env.WEBHOOK_URL,
      back_urls: { success: process.env.FRONTEND_URL || 'https://tresbits.com/success' },
    };

    const response = await axios.post('https://api.mercadopago.com/checkout/preferences', preferencia, {
      headers: { Authorization: `Bearer ${accessToken}` },
    });

    res.json({ link: response.data.init_point, descuento });
  } catch (err) {
    console.error('❌ Error generando link:', err);
    res.status(500).send('Error interno');
  }
});

// 🧠 2. Webhook para procesar pagos
app.post('/webhook', async (req, res) => {
  try {
    const paymentId = req.body.data.id;
    const payment = await mercadopago.payment.get(paymentId);
    const info = payment.body;

    if (info.status !== 'approved') return res.status(200).send('Pago no aprobado');

    const refParts = info.external_reference.split(';');
    const gimnasioId = refParts.find(p => p.startsWith('gim:'))?.split(':')[1];
    const planNombre = refParts.find(p => p.startsWith('plan:'))?.split(':')[1];

    if (!gimnasioId || !planNombre) return res.status(400).send('Referencia inválida');

    const configSnap = await db.collection('gimnasios').doc(gimnasioId).collection('configuracionMercadoPago').doc('datos').get();
    if (!configSnap.exists || info.collector_id.toString() !== configSnap.data().collectorId.toString())
      return res.status(403).send('Collector ID inválido');

    const planSnap = await db.collection('planesLicencia').where('nombre', '==', planNombre).limit(1).get();
    const plan = !planSnap.empty ? planSnap.docs[0].data() : { duracion: 30 };

    const licenciaRef = db.collection('gimnasios').doc(gimnasioId).collection('licencia').doc('datos');
    const licenciaSnap = await licenciaRef.get();

    const ahora = new Date();
    let inicio = ahora;

    if (licenciaSnap.exists) {
      const vencimiento = new Date(licenciaSnap.data().fechaVencimiento);
      if (vencimiento > ahora) inicio = vencimiento;
    }

    const vencimiento = new Date(inicio);
    vencimiento.setDate(vencimiento.getDate() + plan.duracion);

    await licenciaRef.set({
      estadoLicencia: 'activa',
      tipoLicencia: planNombre,
      fechaInicio: inicio.toISOString().split('T')[0],
      fechaVencimiento: vencimiento.toISOString().split('T')[0]
    }, { merge: true });

    await db.collection('gimnasios').doc(gimnasioId)
      .collection('licencia').doc('bitacoraLicencia')
      .collection('entradas').add({
        fechaOperacion: new Date().toISOString(),
        tipo: 'compra_licencia',
        metodo: 'webhook',
        plan: planNombre,
        dias: plan.duracion,
        monto: info.transaction_amount,
        paymentId,
        fechaInicio: inicio.toISOString().split('T')[0],
        fechaVencimiento: vencimiento.toISOString().split('T')[0],
        titulo: info.description || planNombre
      });

    res.status(200).send('✅ Licencia procesada');
  } catch (error) {
    console.error('❌ Webhook error:', error);
    res.status(500).send('Error procesando webhook');
  }
});

// 🚀 Inicio
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🟢 FitSuite Pro Webhook en puerto ${PORT}`));
