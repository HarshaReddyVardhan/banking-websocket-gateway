import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const envSchema = z.object({
    NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
    PORT: z.string().default('8080'),
    METRICS_PORT: z.string().default('9090'),
    REDIS_URL: z.string().default('redis://localhost:6379'),
    KAFKA_BROKERS: z.string().default('localhost:9092'),
    KAFKA_CLIENT_ID: z.string().default('websocket-gateway'),
    KAFKA_GROUP_ID: z.string().default('websocket-gateway-group'),
    JWT_SECRET: z.string().default('secret'),
    JWT_ISSUER: z.string().default('banking-auth-service'),
    JWT_AUDIENCE: z.string().default('banking-websocket-gateway'),
    JWT_ALGORITHMS: z.string().default('HS256,RS256'),
    LOG_LEVEL: z.enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal']).default('info'),

    // Rate Limiting
    RATE_LIMIT_BUCKET_SIZE: z.string().default('10'),
    RATE_LIMIT_REFILL_RATE: z.string().default('1'),
    RATE_LIMIT_REFILL_INTERVAL_MS: z.string().default('1000'),
    MAX_CONNECTIONS_PER_USER: z.string().default('5'),

    // CORS
    CORS_ALLOWED_ORIGINS: z.string().default(''),
    CORS_ALLOWED_METHODS: z.string().default('GET,POST,OPTIONS'),
    CORS_ALLOWED_HEADERS: z.string().default('Content-Type,Authorization'),
    CORS_MAX_AGE: z.string().default('86400'),

    // Timeouts
    REDIS_OPERATION_TIMEOUT_MS: z.string().default('5000'),
    WEBSOCKET_HEARTBEAT_INTERVAL_MS: z.string().default('30000'),
    WEBSOCKET_IDLE_TIMEOUT_MS: z.string().default('300000'),
    KAFKA_RECONNECT_MAX_RETRIES: z.string().default('10'),
    KAFKA_RECONNECT_INITIAL_DELAY_MS: z.string().default('1000'),

    // Circuit Breaker
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: z.string().default('5'),
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS: z.string().default('30000'),
    CIRCUIT_BREAKER_HALF_OPEN_MAX_CALLS: z.string().default('3'),

    // Dead Letter Queue
    KAFKA_DLQ_TOPIC: z.string().default('banking.dlq'),

    // Graceful Shutdown
    SHUTDOWN_TIMEOUT_MS: z.string().default('30000'),
});

const env = envSchema.parse(process.env);

export const config = {
    env: env.NODE_ENV,
    server: {
        port: parseInt(env.PORT, 10),
        metricsPort: parseInt(env.METRICS_PORT, 10),
    },
    redis: {
        url: env.REDIS_URL,
        operationTimeout: parseInt(env.REDIS_OPERATION_TIMEOUT_MS, 10),
    },
    kafka: {
        brokers: env.KAFKA_BROKERS.split(','),
        clientId: env.KAFKA_CLIENT_ID,
        groupId: env.KAFKA_GROUP_ID,
        dlqTopic: env.KAFKA_DLQ_TOPIC,
        reconnect: {
            maxRetries: parseInt(env.KAFKA_RECONNECT_MAX_RETRIES, 10),
            initialDelayMs: parseInt(env.KAFKA_RECONNECT_INITIAL_DELAY_MS, 10),
        },
    },
    auth: {
        jwtSecret: env.JWT_SECRET,
        jwtIssuer: env.JWT_ISSUER,
        jwtAudience: env.JWT_AUDIENCE,
        jwtAlgorithms: env.JWT_ALGORITHMS.split(',') as Array<'HS256' | 'HS384' | 'HS512' | 'RS256' | 'RS384' | 'RS512' | 'ES256' | 'ES384' | 'ES512' | 'PS256' | 'PS384' | 'PS512'>,
    },
    rateLimit: {
        bucketSize: parseInt(env.RATE_LIMIT_BUCKET_SIZE, 10),
        refillRate: parseInt(env.RATE_LIMIT_REFILL_RATE, 10),
        refillIntervalMs: parseInt(env.RATE_LIMIT_REFILL_INTERVAL_MS, 10),
        maxConnectionsPerUser: parseInt(env.MAX_CONNECTIONS_PER_USER, 10),
    },
    cors: {
        allowedOrigins: env.CORS_ALLOWED_ORIGINS ? env.CORS_ALLOWED_ORIGINS.split(',') : [],
        allowedMethods: env.CORS_ALLOWED_METHODS.split(','),
        allowedHeaders: env.CORS_ALLOWED_HEADERS.split(','),
        maxAge: parseInt(env.CORS_MAX_AGE, 10),
    },
    websocket: {
        heartbeatIntervalMs: parseInt(env.WEBSOCKET_HEARTBEAT_INTERVAL_MS, 10),
        idleTimeoutMs: parseInt(env.WEBSOCKET_IDLE_TIMEOUT_MS, 10),
    },
    circuitBreaker: {
        failureThreshold: parseInt(env.CIRCUIT_BREAKER_FAILURE_THRESHOLD, 10),
        recoveryTimeoutMs: parseInt(env.CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS, 10),
        halfOpenMaxCalls: parseInt(env.CIRCUIT_BREAKER_HALF_OPEN_MAX_CALLS, 10),
    },
    shutdown: {
        timeoutMs: parseInt(env.SHUTDOWN_TIMEOUT_MS, 10),
    },
    logging: {
        level: env.LOG_LEVEL,
    },
};
