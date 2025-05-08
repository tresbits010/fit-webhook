const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const admin = require('firebase-admin');

const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });

const db = admin.firestore();
const app = express();
const PORT = process.env.PORT || 3000;

app.use(bodyParser.json());

app.get('/crear-link-pago', async (req, res) => {
  const gimnasioId = req.query.gimnasioId;
  const plan = req.query.plan;
  const ref = req.query.ref || null;

  if (!gimnasioId || !plan) {
    return res.status(400).send('Faltan parametros: gimnasioId y plan');
  }

  try {
    const planDoc = await db.collection('planesLicencia').doc(plan).get();
    if (!planDoc.exists) return res.status(404).send("El plan no existe");

    const data = planDoc.data();
    let precio = data.precio;
    const duracion = data.duracion;
    const nombrePlan = data.nombre || plan;

    let descuento = 0;
    if (ref) {
      const refDoc = await db.collection('referidos').doc(ref).get();
      if (refDoc.exists) {
        descuento = refDoc.data().porcentaje || 0;
        await db.collection('referidos').doc(ref).collection('usos').add({
          gimnasioId,
          plan,
          fecha: new Date().toISOString()
        });
      }
    }

    const configDoc = await db.collection('gimnasios').doc(gimnasioId).collection('configuracionMercadoPago').doc('datos').get();
    if (!configDoc.exists) return res.status(400).send('Configuración MP no encontrada');

    const config = configDoc.data();
    const accessToken = config.accessToken;

    const precioFinal = Math.round(precio * (1 - descuento / 100));
    const title = `Licencia ${nombrePlan} (${duracion} días)`;

    const preferencia = {
      items: [{
        title,
        quantity: 1,
        currency_id: "ARS",
        unit_price: precioFinal
      }],
      external_reference: `tipo:licencia;gim:${gimnasioId};plan:${nombrePlan}`,
      notification_url: "https://fit-webhook.onrender.com/webhook"
    };

    const response = await axios.post('https://api.mercadopago.com/checkout/preferences', preferencia, {
      headers: {
        Authorization: `Bearer ${accessToken}`
      }
    });

    res.send(response.data.init_point);
  } catch (err) {
    console.error("❌ Error al crear link de pago:", err);
    res.status(500).send('Error generando preferencia');
  }
});

app.get('/crear-link-venta', async (req, res) => {
  const { gimnasioId, dni, producto, precio } = req.query;

  if (!gimnasioId || !dni || !producto || !precio) {
    return res.status(400).send('Faltan parámetros: gimnasioId, dni, producto, precio');
  }

  try {
    const configDoc = await db.collection('gimnasios').doc(gimnasioId).collection('configuracionMercadoPago').doc('datos').get();
    if (!configDoc.exists) return res.status(400).send('Configuración MP no encontrada');

    const config = configDoc.data();
    const accessToken = config.accessToken;

    const title = producto;

    const preferencia = {
      items: [{
        title,
        quantity: 1,
        currency_id: "ARS",
        unit_price: parseFloat(precio)
      }],
      external_reference: `tipo:venta;gim:${gimnasioId};dni:${dni};producto:${producto}`,
      notification_url: "https://fit-webhook.onrender.com/webhook"
    };

    const response = await axios.post('https://api.mercadopago.com/checkout/preferences', preferencia, {
      headers: {
        Authorization: `Bearer ${accessToken}`
      }
    });

    res.send(response.data.init_point);
  } catch (err) {
    console.error("❌ Error al generar link de venta:", err);
    res.status(500).send('Error generando link');
  }
});

app.post('/webhook', async (req, res) => {
  try {
    const paymentId = req.body.data.id;
    const response = await axios.get(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
      headers: {
        Authorization: `Bearer ${process.env.ACCESS_TOKEN_MP}`
      }
    });

    const payment = response.data;
    if (payment.status !== 'approved') return res.status(200).send('No procesado');

    const reference = payment.external_reference;
    const gimnasioIdMatch = reference.match(/gim:([^;]+)/);
    if (!gimnasioIdMatch) return res.status(400).send('Falta gimnasioId');
    const gimnasioId = gimnasioIdMatch[1];

    const configDoc = await db.collection('gimnasios').doc(gimnasioId).collection('configuracionMercadoPago').doc('datos').get();
    if (!configDoc.exists) return res.status(403).send('Config MP no encontrada');

    const config = configDoc.data();
    const expectedCollector = config.collectorId;
    if (payment.collector_id.toString() !== expectedCollector.toString()) {
      console.warn(`⚠️ collector_id inválido para ${gimnasioId}`);
      return res.status(403).send('CollectorId no coincide');
    }

    if (reference.startsWith('tipo:venta')) {
      const partes = reference.split(';');
      const dniCliente = partes.find(p => p.startsWith('dni:')).split(':')[1];
      const producto = partes.find(p => p.startsWith('producto:')).split(':')[1];

      const venta = {
        producto,
        precio: payment.transaction_amount,
        cliente: payment.payer?.email || "cliente",
        dniCliente,
        fecha: new Date().toISOString(),
        estado: "pendiente",
        titulo: payment.additional_info?.items?.[0]?.title || producto
      };

      await db.collection('gimnasios').doc(gimnasioId).collection('ventas').add(venta);
      console.log(`🛒 Venta registrada: ${producto} (${dniCliente})`);
    } else {
      const partes = reference.split(';');
      const planNombre = partes.find(p => p.startsWith('plan:')).split(':')[1];

      const planDoc = await db.collection('planesLicencia').where('nombre', '==', planNombre).limit(1).get();
      const planData = !planDoc.empty ? planDoc.docs[0].data() : { duracion: 30 };
      const dias = planData.duracion;

      const now = new Date();
      let fechaInicio = now;

      const licenciaRef = db.collection('gimnasios').doc(gimnasioId).collection('licencia').doc('datos');
      const licenciaSnap = await licenciaRef.get();

      if (licenciaSnap.exists) {
        const vencStr = licenciaSnap.get("fechaVencimiento");
        if (vencStr) {
          const vencActual = new Date(vencStr);
          if (vencActual > now) {
            fechaInicio = vencActual;
          }
        }
      }

      const fechaVencimiento = new Date(fechaInicio);
      fechaVencimiento.setDate(fechaVencimiento.getDate() + dias);

      const titulo = payment.additional_info?.items?.[0]?.title || planNombre;

      await licenciaRef.set({
        estadoLicencia: 'activa',
        tipoLicencia: planNombre,
        fechaInicio: fechaInicio.toISOString().split('T')[0],
        fechaVencimiento: fechaVencimiento.toISOString().split('T')[0]
      }, { merge: true });

      await db.collection('gimnasios').doc(gimnasioId)
        .collection('licencia').doc('bitacoraLicencia').collection('entradas')
        .add({
          fechaOperacion: new Date().toISOString(),
          tipo: 'compra_licencia',
          metodo: 'webhook',
          plan: planNombre,
          dias,
          monto: payment.transaction_amount,
          paymentId,
          fechaInicio: fechaInicio.toISOString().split('T')[0],
          fechaVencimiento: fechaVencimiento.toISOString().split('T')[0],
          titulo
        });

      console.log(`✅ Licencia activada para ${gimnasioId} (${dias} días sumados desde ${fechaInicio.toISOString().split('T')[0]})`);
    }

    res.status(200).send('Webhook recibido');
  } catch (err) {
    console.error("❌ Error en webhook:", err.message);
    res.status(500).send('Error en webhook');
  }
});

app.listen(PORT, () => {
  console.log(`🟢 Webhook activo en puerto ${PORT}`);
});
