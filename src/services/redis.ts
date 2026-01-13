import Redis from 'ioredis';
import { config } from '../config/config';
import { logger } from '../utils/logger';
import { redisCircuitBreaker, CircuitOpenError } from './circuit-breaker';
import { withTimeout } from '../middleware/panic-recovery';

export class RedisService {
    private static instance: RedisService;
    private pubClient: Redis;
    private subClient: Redis;
    private isConnected: boolean = false;
    private isShuttingDown: boolean = false;

    private constructor() {
        const redisOptions = {
            retryStrategy: (times: number) => {
                if (this.isShuttingDown) return null;
                const delay = Math.min(times * 1000, 30000);
                logger.warn({ attempt: times, delayMs: delay }, 'Redis reconnecting');
                return delay;
            },
            maxRetriesPerRequest: 3,
            enableReadyCheck: true,
            lazyConnect: false,
        };

        this.pubClient = new Redis(config.redis.url, redisOptions);
        this.subClient = new Redis(config.redis.url, redisOptions);

        this.setupEventHandlers(this.pubClient, 'Pub');
        this.setupEventHandlers(this.subClient, 'Sub');
    }

    /**
     * Setup Redis event handlers
     */
    private setupEventHandlers(client: Redis, name: string): void {
        client.on('connect', () => {
            logger.info(`Redis ${name} Client connecting`);
        });

        client.on('ready', () => {
            logger.info(`Redis ${name} Client ready`);
            this.isConnected = true;
        });

        client.on('error', (err) => {
            logger.error({ err }, `Redis ${name} Client Error`);
        });

        client.on('close', () => {
            logger.warn(`Redis ${name} Client connection closed`);
            this.isConnected = false;
        });

        client.on('reconnecting', () => {
            logger.info(`Redis ${name} Client reconnecting`);
        });

        client.on('end', () => {
            logger.info(`Redis ${name} Client connection ended`);
            this.isConnected = false;
        });
    }

    public static getInstance(): RedisService {
        if (!RedisService.instance) {
            RedisService.instance = new RedisService();
        }
        return RedisService.instance;
    }

    public getPubClient(): Redis {
        return this.pubClient;
    }

    public getSubClient(): Redis {
        return this.subClient;
    }

    /**
     * Safe GET with circuit breaker and timeout
     */
    public async safeGet(key: string): Promise<string | null> {
        try {
            return await redisCircuitBreaker.execute(async () => {
                return withTimeout(
                    this.pubClient.get(key),
                    config.redis.operationTimeout,
                    `Redis GET ${key}`
                );
            });
        } catch (error) {
            if (error instanceof CircuitOpenError) {
                logger.warn({ key }, 'Redis circuit breaker is open, skipping GET');
                return null;
            }
            throw error;
        }
    }

    /**
     * Safe SET with circuit breaker and timeout
     */
    public async safeSet(key: string, value: string, ttlSeconds?: number): Promise<void> {
        try {
            await redisCircuitBreaker.execute(async () => {
                const operation = ttlSeconds
                    ? this.pubClient.setex(key, ttlSeconds, value)
                    : this.pubClient.set(key, value);

                await withTimeout(
                    operation,
                    config.redis.operationTimeout,
                    `Redis SET ${key}`
                );
            });
        } catch (error) {
            if (error instanceof CircuitOpenError) {
                logger.warn({ key }, 'Redis circuit breaker is open, skipping SET');
                return;
            }
            throw error;
        }
    }

    /**
     * Safe DELETE with circuit breaker and timeout
     */
    public async safeDel(key: string): Promise<number> {
        try {
            return await redisCircuitBreaker.execute(async () => {
                return withTimeout(
                    this.pubClient.del(key),
                    config.redis.operationTimeout,
                    `Redis DEL ${key}`
                );
            });
        } catch (error) {
            if (error instanceof CircuitOpenError) {
                logger.warn({ key }, 'Redis circuit breaker is open, skipping DEL');
                return 0;
            }
            throw error;
        }
    }

    /**
     * Publish message to a channel
     */
    public async publish(channel: string, message: string): Promise<number> {
        try {
            return await redisCircuitBreaker.execute(async () => {
                return withTimeout(
                    this.pubClient.publish(channel, message),
                    config.redis.operationTimeout,
                    `Redis PUBLISH ${channel}`
                );
            });
        } catch (error) {
            if (error instanceof CircuitOpenError) {
                logger.warn({ channel }, 'Redis circuit breaker is open, skipping PUBLISH');
                return 0;
            }
            logger.error({ error, channel }, 'Failed to publish message');
            return 0;
        }
    }

    /**
     * Subscribe to a channel
     */
    public async subscribe(channel: string, callback: (message: string) => void): Promise<void> {
        try {
            await this.subClient.subscribe(channel);

            this.subClient.on('message', (chn, msg) => {
                if (chn === channel) {
                    try {
                        callback(msg);
                    } catch (error) {
                        logger.error({ error, channel }, 'Error in subscription callback');
                    }
                }
            });

            logger.info({ channel }, 'Subscribed to Redis channel');
        } catch (error) {
            logger.error({ error, channel }, 'Failed to subscribe to channel');
            throw error;
        }
    }

    /**
     * Ping Redis to check connectivity
     */
    public async ping(): Promise<boolean> {
        try {
            const result = await withTimeout(
                this.pubClient.ping(),
                config.redis.operationTimeout,
                'Redis PING'
            );
            return result === 'PONG';
        } catch (error) {
            logger.error({ error }, 'Redis ping failed');
            return false;
        }
    }

    /**
     * Check if Redis is connected
     */
    public isHealthy(): boolean {
        return this.isConnected && this.pubClient.status === 'ready';
    }

    /**
     * Gracefully disconnect from Redis
     */
    public async disconnect(): Promise<void> {
        this.isShuttingDown = true;

        try {
            await Promise.all([
                this.pubClient.quit(),
                this.subClient.quit(),
            ]);
            logger.info('Redis clients disconnected');
        } catch (error) {
            logger.error({ error }, 'Error disconnecting Redis clients');
            // Force disconnect
            this.pubClient.disconnect();
            this.subClient.disconnect();
        }
    }
}
