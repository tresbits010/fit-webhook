// server.js
const express = require('express');
const mercadopago = require('mercadopago');
const admin = require('firebase-admin');
const dotenv = require('dotenv');
dotenv.config();

// Inicialización Firebase
const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

// Inicialización MercadoPago
mercadopago.configure({ access_token: process.env.MP_ACCESS_TOKEN });

const app = express();
app.use(express.json());

// 🧾 CREAR LINK DE PAGO
app.get('/crear-link-pago', async (req, res) => {
    const { gimnasioId, plan, ref } = req.query;
    if (!gimnasioId || !plan) return res.status(400).send('Faltan parametros');

    try {
        const planDoc = await db.collection('planesLicencia').doc(plan).get();
        if (!planDoc.exists) return res.status(404).send('Plan no encontrado');

        const datos = planDoc.data();
        const preference = {
            items: [{
                title: `Licencia ${datos.nombre}`,
                unit_price: datos.precio,
                quantity: 1
            }],
            external_reference: `gym:${gimnasioId}|plan:${plan}|ref:${ref || ''}`,
            back_urls: {
                success: 'https://fit-webhook.onrender.com/success',
                failure: 'https://fit-webhook.onrender.com/failure'
            },
            auto_return: 'approved'
        };

        const result = await mercadopago.preferences.create(preference);
        return res.send(result.body.init_point);
    } catch (e) {
        console.error('Error al generar link:', e);
        return res.status(500).send('Error interno');
    }
});

// 📬 WEBHOOK PARA LICENCIAS
app.post('/webhook', async (req, res) => {
    try {
        console.log('📩 Webhook recibido:', JSON.stringify(req.body, null, 2));

        const paymentId = req.body?.data?.id;
        if (!paymentId) return res.status(400).send('Sin ID de pago');

        const payment = await mercadopago.payment.get(paymentId);
        if (payment.body.status !== 'approved') return res.status(200).send('Pago no aprobado');

        const extRef = payment.body.external_reference;
        if (!extRef) return res.status(400).send('Falta external_reference');

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
                const vencimiento = licenciaSnap.data().fechaVencimiento?.toDate?.() || new Date(licenciaSnap.data().fechaVencimiento);
                if (vencimiento > fechaActual) fechaInicio = vencimiento;
            }

            const fechaVencimiento = new Date(fechaInicio);
            fechaVencimiento.setDate(fechaVencimiento.getDate() + duracion);

            const montoPagado = payment.body.transaction_amount;
            const descuentoAplicado = Math.round((1 - (montoPagado / montoOriginal)) * 100);

            transaction.set(licenciaRef, {
                estado: 'activa',
                plan: planId,
                planNombre: plan.nombre,
                fechaInicio,
                fechaVencimiento,
                ultimaActualizacion: admin.firestore.FieldValue.serverTimestamp(),
                usoTrial: false
            }, { merge: true });

            transaction.set(gymRef.collection('transacciones').doc(paymentId), {
                monto: montoPagado,
                fecha: admin.firestore.FieldValue.serverTimestamp(),
                metodo: payment.body.payment_type_id,
                referidoDe,
                descuentoAplicado,
                detalle: `Licencia ${planId} - ${payment.body.description}`
            });

            transaction.set(gymRef.collection('licenciaHistorial').doc(), {
                fecha: admin.firestore.FieldValue.serverTimestamp(),
                plan: planId,
                referidoDe,
                descuentoAplicado,
                montoPagado
            });

            if (referidoDe) {
                const refDoc = db.collection('referidos').doc(referidoDe);
                transaction.set(refDoc, { descuentoAcumulado: admin.firestore.FieldValue.increment(descuentoAplicado) }, { merge: true });
            }
        });

        await admin.messaging().sendToTopic(gimnasioId, {
            notification: {
                title: '🎉 ¡Licencia Renovada!',
                body: `Plan activo hasta el ${new Date().toLocaleDateString()}`
            }
        });

        res.status(200).send('OK');
    } catch (error) {
        console.error('❌ Error en webhook:', error);
        res.status(500).send('Error procesando pago');
    }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`🚀 Webhook activo en puerto ${PORT}`));
