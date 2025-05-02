const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const admin = require('firebase-admin');

// ✅ Usar clave desde variable de entorno
const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });

const db = admin.firestore();
const app = express();
const PORT = process.env.PORT || 3000;

app.use(bodyParser.json());

// ✅ Crear link de pago para licencia
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

    const precioFinal = Math.round(precio * (1 - descuento / 100));

    const preferencia = {
      items: [{
        title: `Licencia ${plan} (${duracion} días)`,
        quantity: 1,
        currency_id: "ARS",
        unit_price: precioFinal
      }],
      external_reference: `gimnasioId:${gimnasioId};plan:${plan}`,
      notification_url: "https://fit-webhook.onrender.com/webhook"
    };

    const response = await axios.post('https://api.mercadopago.com/checkout/preferences', preferencia, {
      headers: {
        Authorization: `Bearer ${process.env.ACCESS_TOKEN_MP}`
      }
    });

    res.send(response.data.init_point);
  } catch (err) {
    console.error("❌ Error al crear link de pago:", err);
    res.status(500).send('Error generando preferencia');
  }
});

// ✅ Crear link de pago para venta de producto
app.get('/crear-link-venta', async (req, res) => {
  const { gimnasioId, dni, producto, precio } = req.query;

  if (!gimnasioId || !dni || !producto || !precio) {
    return res.status(400).send('Faltan parámetros: gimnasioId, dni, producto, precio');
  }

  try {
    const preferencia = {
      items: [{
        title: producto,
        quantity: 1,
        currency_id: "ARS",
        unit_price: parseFloat(precio)
      }],
      external_reference: `tipo:venta;gim:${gimnasioId};dni:${dni};producto:${producto}`,
      notification_url: "https://fit-webhook.onrender.com/webhook"
    };

    const response = await axios.post('https://api.mercadopago.com/checkout/preferences', preferencia, {
      headers: {
        Authorization: `Bearer ${process.env.ACCESS_TOKEN_MP}`
      }
    });

    res.send(response.data.init_point);
  } catch (err) {
    console.error("❌ Error al generar link de venta:", err);
    res.status(500).send('Error generando link');
  }
});

// ✅ Webhook MercadoPago
app.post('/webhook', async (req, res) => {
  try {
    const paymentId = req.body.data.id;
    const response = await axios.get(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
      headers: {
        Authorization: `Bearer ${process.env.ACCESS_TOKEN_MP}`
      }
    });

    const payment = response.data;
    if (payment.status === 'approved') {
      const reference = payment.external_reference;

      if (reference.startsWith('tipo:venta')) {
        const partes = reference.split(';');
        const gimnasioId = partes.find(p => p.startsWith('gim:')).split(':')[1];
        const dniCliente = partes.find(p => p.startsWith('dni:')).split(':')[1];
        const producto = partes.find(p => p.startsWith('producto:')).split(':')[1];

        const venta = {
          producto,
          precio: payment.transaction_amount,
          cliente: payment.payer?.email || "cliente",
          dniCliente,
          fecha: new Date().toISOString(),
          estado: "pendiente"
        };

        await db.collection('gimnasios').doc(gimnasioId).collection('ventas').add(venta);
        console.log(`🛒 Venta registrada: ${producto} (${dniCliente})`);
      } else {
        const partes = reference.split(';');
        const gimnasioId = partes[0].split(':')[1];
        const plan = partes[1].split(':')[1];

        const planDoc = await db.collection('planesLicencia').doc(plan).get();
        const dias = planDoc.exists ? planDoc.data().duracion : 30;

        const fechaInicio = new Date();
        const fechaVencimiento = new Date();
        fechaVencimiento.setDate(fechaInicio.getDate() + dias);

        await db.collection('gimnasios')
                .doc(gimnasioId)
                .collection('licencia')
                .doc('datos')
                .set({
                  estadoLicencia: 'activa',
                  tipoLicencia: plan,
                  fechaInicio: fechaInicio.toISOString().split('T')[0],
                  fechaVencimiento: fechaVencimiento.toISOString().split('T')[0]
                }, { merge: true });

        console.log(`✅ Licencia activada para ${gimnasioId}`);
      }
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
