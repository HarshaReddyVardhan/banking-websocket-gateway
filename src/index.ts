import express from 'express';
import http from 'http';
import { config } from './config/config';
import { logger } from './utils/logger';
import { WebSocketGateway } from './services/websocket-gateway';
import { KafkaConsumerService } from './services/kafka-consumer';
import { RedisService } from './services/redis';
import { ConnectionManager } from './services/connection-manager';
import { register, collectDefaultMetrics, Gauge, Counter } from 'prom-client';
import { healthChecker } from './services/health-checker';
import { circuitBreakerRegistry } from './services/circuit-breaker';
import { corsMiddleware, securityHeadersMiddleware } from './middleware/cors';
import { rateLimitMiddleware } from './middleware/rate-limiter';
import { httpAuditMiddleware } from './middleware/audit-logger';
import { setupGlobalErrorHandlers, errorHandler, asyncHandler } from './middleware/panic-recovery';

// Setup global error handlers FIRST
setupGlobalErrorHandlers();

const app = express();
const server = http.createServer(app);

// Initialize Prometheus Metrics
collectDefaultMetrics();

// Custom metrics
const wsConnections = new Gauge({
    name: 'websocket_connections_total',
    help: 'Total number of active WebSocket connections',
});

const wsConnectionsPerUser = new Gauge({
    name: 'websocket_connections_per_user',
    help: 'Number of WebSocket connections per user',
    labelNames: ['user_id'],
});

const messagesReceived = new Counter({
    name: 'kafka_messages_received_total',
    help: 'Total Kafka messages received',
    labelNames: ['topic'],
});

// Apply security middleware
app.use(securityHeadersMiddleware);
app.use(corsMiddleware);
app.use(httpAuditMiddleware);

// Apply rate limiting to HTTP endpoints
app.use('/api', rateLimitMiddleware());

// Parse JSON body
app.use(express.json({ limit: '10kb' }));

// Initialize Services
let wsGateway: WebSocketGateway;
let kafkaConsumer: KafkaConsumerService;
let connectionManager: ConnectionManager;
let redisService: RedisService;

// Health Checks
app.get('/health', (req, res) => {
    const liveness = healthChecker.isAlive();
    res.status(200).json({
        status: 'ok',
        ...liveness,
    });
});

app.get('/ready', asyncHandler(async (req, res) => {
    const health = await healthChecker.checkAll();
    const statusCode = health.healthy ? 200 : 503;
    res.status(statusCode).json(health);
}));

// Detailed status endpoint (for debugging)
app.get('/status', asyncHandler(async (req, res) => {
    const connectionMetrics = connectionManager?.getMetrics() || { totalConnections: 0, activeUsers: 0 };
    const circuitStatus = circuitBreakerRegistry.getAllStatuses();
    const health = await healthChecker.checkAll();

    res.status(200).json({
        health,
        connections: connectionMetrics,
        circuitBreakers: circuitStatus,
    });
}));

// Metrics endpoint
app.get('/metrics', asyncHandler(async (req, res) => {
    try {
        // Update custom metrics
        const stats = connectionManager?.getMetrics();
        if (stats) {
            wsConnections.set(stats.totalConnections);
        }

        res.set('Content-Type', register.contentType);
        res.end(await register.metrics());
    } catch (err) {
        logger.error({ err }, 'Error collecting metrics');
        res.status(500).json({ error: 'Failed to collect metrics' });
    }
}));

// Error handling middleware (must be last)
app.use(errorHandler);

async function start() {
    try {
        // Initialize Redis first
        redisService = RedisService.getInstance();

        // Wait for Redis to be ready (with timeout)
        let redisReady = false;
        for (let i = 0; i < 10 && !redisReady; i++) {
            redisReady = await redisService.ping();
            if (!redisReady) {
                logger.warn({ attempt: i + 1 }, 'Waiting for Redis...');
                await delay(1000);
            }
        }

        if (!redisReady) {
            logger.warn('Redis not ready, continuing without it');
        } else {
            logger.info('Redis connected successfully');
        }

        // Initialize Connection Manager
        connectionManager = ConnectionManager.getInstance();

        // Initialize WebSocket Gateway
        wsGateway = new WebSocketGateway(server);

        // Initialize Kafka Consumer
        kafkaConsumer = new KafkaConsumerService();
        await kafkaConsumer.start();

        // Setup health checker with clients
        healthChecker.setRedisClient(redisService.getPubClient());
        healthChecker.setKafkaClient(kafkaConsumer.getKafkaClient());
        healthChecker.setWebSocketMetrics(() => connectionManager.getConnectionCount());

        // Start server
        server.listen(config.server.port, () => {
            logger.info({
                port: config.server.port,
                env: config.env,
            }, 'WebSocket Gateway started successfully');
        });

        // Update metrics periodically
        setInterval(() => {
            const stats = connectionManager?.getMetrics();
            if (stats) {
                wsConnections.set(stats.totalConnections);
            }
        }, 5000);

    } catch (err) {
        logger.fatal({ err }, 'Failed to start application');
        process.exit(1);
    }
}

// Graceful Shutdown
let isShuttingDown = false;

const shutdown = async (signal: string) => {
    if (isShuttingDown) {
        logger.warn('Shutdown already in progress');
        return;
    }
    isShuttingDown = true;

    logger.info({ signal }, 'Starting graceful shutdown...');

    // Set a hard timeout for shutdown
    const shutdownTimeout = setTimeout(() => {
        logger.error('Shutdown timeout exceeded, forcing exit');
        process.exit(1);
    }, config.shutdown.timeoutMs);

    try {
        // Stop accepting new connections
        server.close();
        logger.info('HTTP server stopped accepting connections');

        // Drain WebSocket connections
        if (connectionManager) {
            await connectionManager.drainConnections();
        }

        // Disconnect Kafka
        if (kafkaConsumer) {
            await kafkaConsumer.disconnect();
        }

        // Disconnect Redis
        if (redisService) {
            await redisService.disconnect();
        }

        clearTimeout(shutdownTimeout);
        logger.info('Graceful shutdown complete');
        process.exit(0);
    } catch (err) {
        logger.error({ err }, 'Error during shutdown');
        clearTimeout(shutdownTimeout);
        process.exit(1);
    }
};

// Handle shutdown signals
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Helper function
function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Start the application
start();
