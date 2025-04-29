const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const admin = require('firebase-admin');

// Inicializar Firebase Admin
const serviceAccount = require('./serviceAccountKey.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(bodyParser.json());

// CREAR LINK PAGO DINÁMICO
app.get('/crear-link-pago', async (req, res) => {
  const gimnasioId = req.query.gimnasioId;
  const plan = req.query.plan;

  if (!gimnasioId || !plan) {
    return res.status(400).send('Faltan parámetros gimnasioId o plan');
  }

  const precio = plan === 'mensual' ? 10000 : (plan === 'anual' ? 100000 : 300000);

  const preferencia = {
    items: [{
      title: `Licencia ${plan}`,
      quantity: 1,
      currency_id: "ARS",
      unit_price: precio
    }],
    external_reference: `gimnasioId:${gimnasioId};plan:${plan}`,
    notification_url: "https://TU_DOMINIO_O_IP/webhook" // donde recibirás notificaciones
  };

  try {
    const response = await axios.post('https://api.mercadopago.com/checkout/preferences', preferencia, {
      headers: {
        Authorization: `Bearer TU_ACCESS_TOKEN_MERCADOPAGO`
      }
    });

    res.send(response.data.init_point); // Link de pago para redirigir
  } catch (error) {
    console.error(error.response.data);
    res.status(500).send('Error creando preferencia');
  }
});

// WEBHOOK PARA ACTUALIZAR LICENCIAS
app.post('/webhook', async (req, res) => {
  try {
    const paymentId = req.body.data.id;

    const response = await axios.get(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
      headers: {
        Authorization: `Bearer TU_ACCESS_TOKEN_MERCADOPAGO`
      }
    });

    const payment = response.data;

    if (payment.status === 'approved') {
      const reference = payment.external_reference;
      const partes = reference.split(';');
      const gimnasioId = partes[0].split(':')[1];
      const plan = partes[1].split(':')[1];

      console.log(`🏋️ Pago aprobado - Gimnasio: ${gimnasioId} - Plan: ${plan}`);

      let diasDuracion = 30;
      if (plan === 'anual') diasDuracion = 365;
      if (plan === 'vitalicio') diasDuracion = 36500;

      const fechaInicio = new Date();
      const fechaVencimiento = new Date();
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

      console.log(`✅ Licencia actualizada para ${gimnasioId}`);
    }

    res.status(200).send('Webhook recibido OK');
  } catch (error) {
    console.error('❌ Error procesando webhook:', error.message);
    res.status(500).send('Error en webhook');
  }
});

app.listen(PORT, () => {
  console.log(`Servidor Webhook escuchando en puerto ${PORT}`);
});
