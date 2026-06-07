import Fastify from 'fastify';
import mongoose from 'mongoose';
import dotenv from 'dotenv';
import cors from 'cors';

dotenv.config();

const fastify = Fastify({ logger: false });

// CORS Setup
fastify.register(import('@fastify/cors'), { 
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE']
});

// Database Connection
const connectDB = async () => {
    try {
        await mongoose.connect(process.env.MONGODB_URI, {
            maxPoolSize: 100,
            minPoolSize: 10,
        });
        console.log('✅ ZENEX Database Connected to Microservice! 🚀');
    } catch (error) {
        console.error('❌ Database Connection Failed:', error);
        process.exit(1);
    }
};

// Test Route
fastify.get('/', async (request, reply) => {
    return { status: 'success', message: 'ZENEX API Server is FLYING! ⚡' };
});

// Start Server
const startServer = async () => {
    try {
        await connectDB();
        const PORT = process.env.PORT || 4000;
        await fastify.listen({ port: PORT, host: '0.0.0.0' });
        console.log(`⚡ ZENEX API is running on http://localhost:${PORT}`);
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
};

startServer();