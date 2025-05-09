const express = require('express');
const bodyParser = require('body-parser');
const { MercadoPagoConfig } = require('mercadopago');
const admin = require('firebase-admin');
const dotenv = require('dotenv');

// Configuración inicial
const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

mercadopago.configure({ access_token: process.env.MP_ACCESS_TOKEN });

const app = express();
app.use(express.json());

// 📌 WEBHOOK DE LICENCIAS AUTOMÁTICAS
app.post('/webhook', async (req, res) => {
    try {
        // Validación de seguridad (si usás firma custom, implementá aquí)
        const paymentId = req.body.data.id;
        const payment = await mercadopago.payment.get(paymentId);

        if (payment.body.status !== 'approved') {
            return res.status(200).send('Pago no aprobado');
        }

        // Desglosar referencia externa: gym:xxx|plan:xxx|ref:xxx
        const [gymPart, planPart, refPart] = payment.body.external_reference.split('|');
        const gymId = gymPart.split(':')[1];
        const planId = planPart.split(':')[1];
        const referidoDe = refPart?.split(':')[1] || null;

        const gymRef = db.collection('gimnasios').doc(gymId);
        const licenciaRef = gymRef.collection('licencia').doc('datos');

        await db.runTransaction(async (transaction) => {
            // Obtener plan
            const planSnap = await db.collection('planesLicencia').doc(planId).get();
            if (!planSnap.exists) throw new Error("Plan no encontrado");

            const duracion = planSnap.data().duracion || 30;
            const planNombre = planSnap.data().nombre || planId;
            const montoOriginal = planSnap.data().precio;

            // Obtener licencia actual
            const licenciaSnap = await transaction.get(licenciaRef);
            const fechaActual = new Date();

            let fechaInicio = fechaActual;
            if (licenciaSnap.exists) {
                const vencimiento = licenciaSnap.data().fechaVencimiento?.toDate();
                if (vencimiento && vencimiento > fechaActual) {
                    fechaInicio = vencimiento;
                }
            }

            const nuevaVencimiento = new Date(fechaInicio);
            nuevaVencimiento.setDate(nuevaVencimiento.getDate() + duracion);

            const montoPagado = payment.body.transaction_amount;
            const descuentoAplicado = Math.round((1 - (montoPagado / montoOriginal)) * 100);

            // ✅ Actualizar licencia
            transaction.set(licenciaRef, {
                estado: 'activa',
                plan: planId,
                planNombre,
                fechaInicio,
                fechaVencimiento: nuevaVencimiento,
                ultimaActualizacion: admin.firestore.FieldValue.serverTimestamp()
            }, { merge: true });

            // ✅ Registrar transacción
            transaction.set(gymRef.collection('transacciones').doc(paymentId), {
                monto: montoPagado,
                fecha: admin.firestore.FieldValue.serverTimestamp(),
                metodo: payment.body.payment_type_id,
                referidoDe,
                descuentoAplicado,
                detalle: `Licencia ${planId} - ${payment.body.description}`
            });

            // ✅ Historial de renovaciones
            transaction.set(gymRef.collection('licenciaHistorial').doc(), {
                fecha: admin.firestore.FieldValue.serverTimestamp(),
                plan: planId,
                referidoDe,
                descuentoAplicado,
                montoPagado
            });

            // ✅ Acumulado del referido (si existe)
            if (referidoDe) {
                const refDoc = db.collection('referidos').doc(referidoDe);
                transaction.update(refDoc, {
                    descuentoAcumulado: admin.firestore.FieldValue.increment(descuentoAplicado)
                });
            }
        });

        // ✅ Notificación push al gimnasio (opcional)
        await admin.messaging().sendToTopic(gymId, {
            notification: {
                title: '🎉 ¡Licencia Renovada!',
                body: `Tu plan ${planId} está activo hasta ${nuevaVencimiento.toLocaleDateString()}`
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
