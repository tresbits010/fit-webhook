const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const admin = require('firebase-admin');
const { v4: uuidv4 } = require('uuid');
const { validateWebhookSignature } = require('mercadopago');

// Configuración inicial
const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();
const app = express();
const PORT = process.env.PORT || 3000;

// Middlewares
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Validación de firma de webhook
const validateMPWebhook = (req, res, next) => {
  const signature = req.headers['x-signature'];
  const publicKey = process.env.MP_PUBLIC_KEY;
  
  if (!validateWebhookSignature(signature, publicKey, req.body)) {
    return res.status(403).send('Firma inválida');
  }
  next();
};

// Helpers de Firestore
const getMPConfig = async (gymId) => {
  const doc = await db.collection('gimnasios').doc(gymId)
    .collection('configuracionMercadoPago').doc('datos').get();
  return doc.exists ? doc.data() : null;
};

// Procesamiento de pagos
const processLicensePayment = async (gymId, planData, payment) => {
  const batch = db.batch();
  const licenseRef = db.collection('gimnasios').doc(gymId).collection('licencia').doc('datos');
  const logRef = db.collection('gimnasios').doc(gymId)
    .collection('licencia').doc('bitacoraLicencia').collection('entradas').doc(uuidv4());

  const licenseDoc = await licenseRef.get();
  const currentExpiration = licenseDoc.exists ? licenseDoc.get('fechaVencimiento') : null;
  const startDate = currentExpiration && new Date(currentExpiration) > new Date() 
    ? new Date(currentExpiration) 
    : new Date();

  const expirationDate = new Date(startDate);
  expirationDate.setDate(expirationDate.getDate() + planData.duracion);

  batch.set(licenseRef, {
    estadoLicencia: 'activa',
    tipoLicencia: planData.nombre,
    fechaInicio: admin.firestore.Timestamp.fromDate(startDate),
    fechaVencimiento: admin.firestore.Timestamp.fromDate(expirationDate)
  }, { merge: true });

  batch.set(logRef, {
    fechaOperacion: admin.firestore.FieldValue.serverTimestamp(),
    tipo: 'compra_licencia',
    metodo: 'webhook',
    plan: planData.nombre,
    dias: planData.duracion,
    monto: payment.transaction_amount,
    paymentId: payment.id,
    fechaInicio: admin.firestore.Timestamp.fromDate(startDate),
    fechaVencimiento: admin.firestore.Timestamp.fromDate(expirationDate)
  });

  await batch.commit();
  return expirationDate;
};

// Endpoints
app.post('/webhook', validateMPWebhook, async (req, res) => {
  try {
    const { data: payment } = await axios.get(
      `https://api.mercadopago.com/v1/payments/${req.body.data.id}`,
      { headers: { Authorization: `Bearer ${process.env.MP_ACCESS_TOKEN}` } }
    );

    if (payment.status !== 'approved') {
      return res.status(200).json({ status: 'ignored', reason: 'payment_not_approved' });
    }

    const [type, gymId] = payment.external_reference.split('|');
    if (!type || !gymId) {
      return res.status(400).send('Referencia inválida');
    }

    const mpConfig = await getMPConfig(gymId);
    if (!mpConfig || payment.collector_id.toString() !== mpConfig.collectorId.toString()) {
      return res.status(403).send('Configuración inválida');
    }

    switch (type) {
      case 'license': {
        const planDoc = await db.collection('planesLicencia').doc(payment.description).get();
        if (!planDoc.exists) throw new Error('Plan no encontrado');
        
        const expirationDate = await processLicensePayment(gymId, planDoc.data(), payment);
        console.log(`✅ Licencia actualizada para ${gymId}, válida hasta ${expirationDate}`);
        break;
      }

      case 'sale': {
        const saleData = {
          producto: payment.description,
          precio: payment.transaction_amount,
          cliente: payment.payer.email,
          dni: payment.metadata.dni,
          fecha: admin.firestore.FieldValue.serverTimestamp(),
          estado: 'completado'
        };
        
        await db.collection('gimnasios').doc(gymId).collection('ventas').add(saleData);
        console.log(`🛒 Venta registrada: ${payment.description} (${payment.metadata.dni})`);
        break;
      }

      default:
        throw new Error('Tipo de transacción no soportado');
    }

    res.status(200).json({ status: 'processed' });
  } catch (error) {
    console.error('❌ Error en webhook:', error.message);
    res.status(500).json({ 
      status: 'error',
      error: error.message,
      stack: process.env.NODE_ENV === 'production' ? undefined : error.stack
    });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Servidor webhook ejecutándose en puerto ${PORT}`);
});
