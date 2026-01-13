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
    JWT_SECRET: z.string().default('secret'), // In prod, this should be a strong secret
    LOG_LEVEL: z.enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal']).default('info'),
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
    },
    kafka: {
        brokers: env.KAFKA_BROKERS.split(','),
        clientId: env.KAFKA_CLIENT_ID,
        groupId: env.KAFKA_GROUP_ID,
    },
    auth: {
        jwtSecret: env.JWT_SECRET,
    },
    logging: {
        level: env.LOG_LEVEL,
    },
};
