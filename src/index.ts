import express from 'express';
import http from 'http';
import { config } from './config/config';
import { logger } from './utils/logger';
import { WebSocketGateway } from './services/websocket-gateway';
import { KafkaConsumerService } from './services/kafka-consumer';
import { register, collectDefaultMetrics } from 'prom-client';
import { ConnectionManager } from './services/connection-manager';

const app = express();
const server = http.createServer(app);

// Initialize Prometheus Metrics
collectDefaultMetrics();

// Initialize Services
const wsGateway = new WebSocketGateway(server);
const kafkaConsumer = new KafkaConsumerService();

// Health Checks
app.get('/health', (req, res) => {
    res.status(200).json({ status: 'ok' });
});

app.get('/ready', (req, res) => {
    // Check Kafka/Redis connection status ideally
    res.status(200).json({ status: 'ready' });
});

app.get('/metrics', async (req, res) => {
    try {
        const stats = ConnectionManager.getInstance().getMetrics();
        // Add custom metrics if defined, or just return register content
        // In a real app we'd create Gauges for connection counts
        res.set('Content-Type', register.contentType);
        res.end(await register.metrics());
    } catch (err) {
        res.status(500).end(err);
    }
});

async function start() {
    try {
        // Start Kafka Consumer
        await kafkaConsumer.start();

        server.listen(config.server.port, () => {
            logger.info({ port: config.server.port }, 'WebSocket Gateway started');
        });

    } catch (err) {
        logger.fatal({ err }, 'Failed to start application');
        process.exit(1);
    }
}

// Graceful Shutdown
const shutdown = async () => {
    logger.info('Shutting down...');
    server.close();
    await kafkaConsumer.disconnect();
    process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

start();
