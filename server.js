const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const admin = require('firebase-admin');

// Inicializar Firebase Admin
const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});
const db = admin.firestore();

const app = express();
const PORT = process.env.PORT || 3000;
app.use(bodyParser.json());

// ✅ CREAR LINK DE PAGO DINÁMICO
app.get('/crear-link-pago', async (req, res) => {
  const gimnasioId = req.query.gimnasioId;
  const plan = req.query.plan;
  const referidoPor = req.query.ref;

  if (!gimnasioId || !plan) {
    return res.status(400).send('Faltan parámetros gimnasioId o plan');
  }

  // Leer precio desde Firestore (si está cargado)
  let precio = 10000; // default
  try {
    const planDoc = await db.collection('planesLicencia').doc(plan).get();
    if (planDoc.exists) {
      const data = planDoc.data();
      precio = data.precio || precio;
    }
  } catch {
    // usar precio default si falla
  }

  // Aplicar descuento por referidos válidos
  if (referidoPor && referidoPor !== gimnasioId) {
    try {
      const refDoc = await db.collection("referidos").doc(referidoPor).get();
      if (refDoc.exists) {
        const usos = refDoc.data().usosValidos || 0;
        const descuento = Math.min(usos, 30);
        precio = Math.floor(precio * (1 - descuento / 100));
      }
    } catch (e) {
      console.log("❌ Error aplicando descuento por referidos:", e.message);
    }
  }

  const preferencia = {
    items: [{
      title: `Licencia ${plan}`,
      quantity: 1,
      currency_id: "ARS",
      unit_price: precio
    }],
    external_reference: `gimnasioId:${gimnasioId};plan:${plan}`,
    notification_url: "https://fit-webhook.onrender.com/webhook" // CAMBIAR si usás otro dominio
  };

  try {
    const response = await axios.post('https://api.mercadopago.com/checkout/preferences', preferencia, {
      headers: {
        Authorization: `Bearer ${process.env.ACCESS_TOKEN_MERCADOPAGO}`
      }
    });

    res.send(response.data.init_point);
  } catch (error) {
    console.error(error.response?.data || error.message);
    res.status(500).send('Error creando preferencia');
  }
});

// ✅ WEBHOOK PARA APROBAR Y ACTIVAR LICENCIA + REFERIDOS
app.post('/webhook', async (req, res) => {
  try {
    const paymentId = req.body.data.id;
    const mp = await axios.get(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
      headers: {
        Authorization: `Bearer ${process.env.ACCESS_TOKEN_MERCADOPAGO}`
      }
    });

    const payment = mp.data;
    if (payment.status === 'approved') {
      const reference = payment.external_reference;
      const partes = reference.split(';');
      const gimnasioId = partes[0].split(':')[1];
      const plan = partes[1].split(':')[1];

      console.log(`✅ Pago aprobado - Gimnasio: ${gimnasioId} - Plan: ${plan}`);

      let diasDuracion = 30;
      if (plan === 'anual') diasDuracion = 365;
      if (plan === 'vitalicio') diasDuracion = 36500;

      const fechaInicio = new Date();
      const fechaVencimiento = new Date(fechaInicio);
      fechaVencimiento.setDate(fechaInicio.getDate() + diasDuracion);

      await db.collection('gimnasios')
        .doc(gimnasioId)
        .collection('licencia')
        .doc('datos')
        .set({
          estadoLicencia: 'activa',
          fechaInicio: fechaInicio.toISOString().split('T')[0],
          fechaVencimiento: fechaVencimiento.toISOString().split('T')[0],
          tipoLicencia: plan
        }, { merge: true });

      // 🧠 REFERIDOS - sólo si vino con ?ref y no es auto-referido
      const url = new URL(payment.additional_info.items[0].description || "", "http://dummy");
      const refParam = url.searchParams.get('ref');
      if (refParam && refParam !== gimnasioId) {
        const usadosRef = db.collection("referidos").doc(refParam).collection("usadosPor").doc(gimnasioId);
        const yaUsado = await usadosRef.get();

        if (!yaUsado.exists) {
          const refDoc = db.collection("referidos").doc(refParam);
          const refData = await refDoc.get();
          const usos = refData.exists ? (refData.data().usosValidos || 0) : 0;

          if (usos < 30) {
            await refDoc.set({ usosValidos: usos + 1 }, { merge: true });
            await usadosRef.set({
              planComprado: plan,
              fecha: new Date().toISOString()
            });

            console.log(`🎁 Referido registrado: ${refParam} ganó 1 uso válido por ${gimnasioId}`);
          }
        }
      }
    }

    res.status(200).send('OK');
  } catch (error) {
    console.error("❌ Webhook error:", error.message);
    res.status(500).send('Error en webhook');
  }
});

app.listen(PORT, () => {
  console.log(`Servidor Webhook escuchando en puerto ${PORT}`);
});

