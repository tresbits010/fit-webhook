const express = require('express');
const bodyParser = require('body-parser');
const mercadopago = require('mercadopago');
const admin = require('firebase-admin');
const dotenv = require('dotenv');

dotenv.config();

const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

mercadopago.configure({ access_token: process.env.MP_ACCESS_TOKEN });

const app = express();
app.use(express.json());

// ✅ CREAR LINK DE PAGO
app.get('/crear-link-pago', async (req, res) => {
    const { gimnasioId, plan, ref } = req.query;
    if (!gimnasioId || !plan) return res.status(400).send('Faltan parámetros');

    try {
        const planSnap = await db.collection('planesLicencia').doc(plan).get();
        if (!planSnap.exists) return res.status(404).send('Plan no encontrado');

        const datos = planSnap.data();
        const preference = {
            items: [{
                title: `Licencia ${datos.nombre || 'Licencia'}`,
                unit_price: datos.precio || 0,
                quantity: 1
            }],
            external_reference: `gym:${gimnasioId}|plan:${plan}|ref:${ref || ''}`,
            back_urls: {
                success: 'https://fit-webhook.onrender.com/success',
                failure: 'https://fit-webhook.onrender.com/failure'
            },
            auto_return: 'approved'
        };

        const response = await mercadopago.preferences.create(preference);
        return res.send(response.body.init_point);
    } catch (error) {
        console.error('❌ Error al generar link:', error);
        return res.status(500).send('Error al generar link de pago');
    }
});

// ✅ PROCESAR PAGO EN WEBHOOK
app.post('/webhook', async (req, res) => {
    try {
        console.log("Webhook recibido:", JSON.stringify(req.body));
        if (!req.body.data?.id) return res.status(400).send('ID no presente');

        const paymentId = req.body.data.id;
        const payment = await mercadopago.payment.get(paymentId);
        if (payment.body.status !== 'approved') return res.status(200).send('Pago no aprobado');

        const reference = payment.body.external_reference;
        const [gymPart, planPart, refPart] = reference.split('|');
        const gimnasioId = gymPart.split(':')[1];
        const planId = planPart.split(':')[1];
        const referidoDe = refPart?.split(':')[1] || null;

        const gymRef = db.collection('gimnasios').doc(gimnasioId);
        const licenciaRef = gymRef.collection('licencia').doc('datos');

        await db.runTransaction(async (transaction) => {
            const planDoc = await db.collection('planesLicencia').doc(planId).get();
            if (!planDoc.exists) throw new Error('Plan inexistente');

            const plan = planDoc.data();
            const duracion = plan.duracion || 30;
            const montoOriginal = plan.precio;
            const planNombre = plan.nombre || planId;

            const now = new Date();
            let fechaInicio = now;

            const licenciaSnap = await transaction.get(licenciaRef);
            if (licenciaSnap.exists) {
                const vencimientoActual = licenciaSnap.get('fechaVencimiento')?.toDate?.() || now;
                if (vencimientoActual > now) fechaInicio = vencimientoActual;
            }

            const nuevaVencimiento = new Date(fechaInicio);
            nuevaVencimiento.setDate(nuevaVencimiento.getDate() + duracion);

            const montoPagado = payment.body.transaction_amount;
            const descuentoAplicado = Math.round((1 - (montoPagado / montoOriginal)) * 100);

            transaction.set(licenciaRef, {
                estado: 'activa',
                plan: planId,
                planNombre,
                fechaInicio,
                fechaVencimiento: nuevaVencimiento,
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
                transaction.set(db.collection('referidos').doc(referidoDe), {
                    descuentoAcumulado: admin.firestore.FieldValue.increment(descuentoAplicado)
                }, { merge: true });
            }
        });

        await admin.messaging().sendToTopic(gimnasioId, {
            notification: {
                title: '🎉 ¡Licencia Renovada!',
                body: `Tu plan ${planId} está activo hasta ${new Date().toLocaleDateString()}`
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
