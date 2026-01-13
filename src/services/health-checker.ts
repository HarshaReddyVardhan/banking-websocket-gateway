import Redis from 'ioredis';
import { Kafka } from 'kafkajs';
import { config } from '../config/config';
import { logger } from '../utils/logger';

/**
 * Health Checker Service
 * 
 * Provides deep health checks for all dependencies:
 * - Redis connectivity
 * - Kafka broker connectivity
 * - WebSocket server status
 */

export interface HealthStatus {
    healthy: boolean;
    timestamp: string;
    uptime: number;
    components: {
        redis: ComponentHealth;
        kafka: ComponentHealth;
        websocket: ComponentHealth;
    };
}

export interface ComponentHealth {
    status: 'healthy' | 'unhealthy' | 'degraded';
    latencyMs?: number;
    message?: string;
    lastCheck: string;
}

class HealthChecker {
    private static instance: HealthChecker;
    private redis: Redis | null = null;
    private kafka: Kafka | null = null;
    private startTime: number = Date.now();
    private lastHealthCheck: HealthStatus | null = null;
    private wsConnectionCount: () => number = () => 0;

    private constructor() { }

    public static getInstance(): HealthChecker {
        if (!HealthChecker.instance) {
            HealthChecker.instance = new HealthChecker();
        }
        return HealthChecker.instance;
    }

    /**
     * Set Redis client for health checks
     */
    public setRedisClient(redis: Redis): void {
        this.redis = redis;
    }

    /**
     * Set Kafka client for health checks
     */
    public setKafkaClient(kafka: Kafka): void {
        this.kafka = kafka;
    }

    /**
     * Set WebSocket connection count provider
     */
    public setWebSocketMetrics(countFn: () => number): void {
        this.wsConnectionCount = countFn;
    }

    /**
     * Check health of all components
     */
    public async checkAll(): Promise<HealthStatus> {
        const [redisHealth, kafkaHealth, wsHealth] = await Promise.all([
            this.checkRedis(),
            this.checkKafka(),
            this.checkWebSocket(),
        ]);

        const healthy =
            redisHealth.status === 'healthy' &&
            kafkaHealth.status === 'healthy' &&
            wsHealth.status === 'healthy';

        const status: HealthStatus = {
            healthy,
            timestamp: new Date().toISOString(),
            uptime: Date.now() - this.startTime,
            components: {
                redis: redisHealth,
                kafka: kafkaHealth,
                websocket: wsHealth,
            },
        };

        this.lastHealthCheck = status;
        return status;
    }

    /**
     * Check Redis connectivity
     */
    private async checkRedis(): Promise<ComponentHealth> {
        if (!this.redis) {
            return {
                status: 'unhealthy',
                message: 'Redis client not configured',
                lastCheck: new Date().toISOString(),
            };
        }

        const startTime = Date.now();
        try {
            const result = await Promise.race([
                this.redis.ping(),
                new Promise<null>((_, reject) =>
                    setTimeout(() => reject(new Error('Timeout')), config.redis.operationTimeout)
                ),
            ]);

            if (result === 'PONG') {
                return {
                    status: 'healthy',
                    latencyMs: Date.now() - startTime,
                    lastCheck: new Date().toISOString(),
                };
            }

            return {
                status: 'unhealthy',
                latencyMs: Date.now() - startTime,
                message: `Unexpected response: ${result}`,
                lastCheck: new Date().toISOString(),
            };
        } catch (error) {
            return {
                status: 'unhealthy',
                latencyMs: Date.now() - startTime,
                message: error instanceof Error ? error.message : 'Unknown error',
                lastCheck: new Date().toISOString(),
            };
        }
    }

    /**
     * Check Kafka broker connectivity
     */
    private async checkKafka(): Promise<ComponentHealth> {
        if (!this.kafka) {
            return {
                status: 'unhealthy',
                message: 'Kafka client not configured',
                lastCheck: new Date().toISOString(),
            };
        }

        const startTime = Date.now();
        const admin = this.kafka.admin();

        try {
            await admin.connect();
            const brokers = await Promise.race([
                admin.describeCluster(),
                new Promise<never>((_, reject) =>
                    setTimeout(() => reject(new Error('Timeout')), 5000)
                ),
            ]);

            await admin.disconnect();

            const activeBrokers = brokers.brokers.length;
            const expectedBrokers = config.kafka.brokers.length;

            if (activeBrokers >= expectedBrokers) {
                return {
                    status: 'healthy',
                    latencyMs: Date.now() - startTime,
                    message: `${activeBrokers} brokers active`,
                    lastCheck: new Date().toISOString(),
                };
            }

            return {
                status: 'degraded',
                latencyMs: Date.now() - startTime,
                message: `Only ${activeBrokers}/${expectedBrokers} brokers active`,
                lastCheck: new Date().toISOString(),
            };
        } catch (error) {
            try {
                await admin.disconnect();
            } catch { } // Ignore disconnect errors

            return {
                status: 'unhealthy',
                latencyMs: Date.now() - startTime,
                message: error instanceof Error ? error.message : 'Unknown error',
                lastCheck: new Date().toISOString(),
            };
        }
    }

    /**
     * Check WebSocket server status
     */
    private async checkWebSocket(): Promise<ComponentHealth> {
        try {
            const connectionCount = this.wsConnectionCount();
            return {
                status: 'healthy',
                message: `${connectionCount} active connections`,
                lastCheck: new Date().toISOString(),
            };
        } catch (error) {
            return {
                status: 'unhealthy',
                message: error instanceof Error ? error.message : 'Unknown error',
                lastCheck: new Date().toISOString(),
            };
        }
    }

    /**
     * Get last health check result (cached)
     */
    public getLastCheck(): HealthStatus | null {
        return this.lastHealthCheck;
    }

    /**
     * Simple liveness check (is the process running?)
     */
    public isAlive(): { alive: boolean; uptime: number } {
        return {
            alive: true,
            uptime: Date.now() - this.startTime,
        };
    }
}

export const healthChecker = HealthChecker.getInstance();
