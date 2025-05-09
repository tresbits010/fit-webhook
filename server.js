const express = require('express');
const bodyParser = require('body-parser');
const { MercadoPagoConfig } = require('mercadopago');
const admin = require('firebase-admin');
const dotenv = require('dotenv');

// Cargar variables de entorno desde .env si existe
dotenv.config();

// 🔐 Configurar Firebase con claves del entorno
const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

// 🔐 Configurar MercadoPago
const mpClient = new MercadoPagoConfig({ accessToken: process.env.MERCADOPAGO_ACCESS_TOKEN });

const app = express();
const PORT = process.env.PORT || 3000;
app.use(bodyParser.json());

// 📌 Endpoint para crear link de pago
app.get('/crear-link-pago', async (req, res) => {
  const gimnasioId = req.query.gimnasioId;
  const plan = req.query.plan;
  const ref = req.query.ref || null;

  if (!gimnasioId || !plan) {
    return res.status(400).send('❌ Faltan parámetros: gimnasioId y plan');
  }

  try {
    const planDoc = await db.collection('planesLicencia').doc(plan).get();
    if (!planDoc.exists) return res.status(404).send('❌ El plan no existe');

    const data = planDoc.data();
    let precio = data.precio;
    const duracion = data.duracion || 30;

    // 🎁 Descuento por referidos
    if (ref) {
      const refDoc = await db.collection('referidos').doc(ref).get();
      if (refDoc.exists) {
        const refData = refDoc.data();
        const comprasValidas = refData.comprasValidas || 0;
        const descuento = Math.min(comprasValidas * 0.01, 0.3); // máx 30%
        precio = Math.round(precio * (1 - descuento));
      }
    }

    // 🧾 Crear preferencia de pago
    const preference = {
      items: [
        {
          title: `Licencia ${data.nombre}`,
          quantity: 1,
          unit_price: precio
        }
      ],
      external_reference: gimnasioId,
      back_urls: {
        success: 'https://fitsuite-pro.web.app/pago-exitoso',
        failure: 'https://fitsuite-pro.web.app/pago-fallido',
        pending: 'https://fitsuite-pro.web.app/pago-pendiente'
      },
      auto_return: 'approved'
    };

    const result = await mpClient.preference.create({ body: preference });

    res.status(200).json({
      link: result.init_point,
      id: result.id,
      precioFinal: precio
    });
  } catch (error) {
    console.error('❌ Error al crear link de pago:', error);
    res.status(500).send('Error interno');
  }
});

// 🟢 Servidor online
app.listen(PORT, () => {
  console.log(`✅ Webhook corriendo en http://localhost:${PORT}`);
});
