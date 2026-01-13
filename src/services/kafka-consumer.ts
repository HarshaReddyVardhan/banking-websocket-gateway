import { Kafka, Consumer, logLevel, Producer, CompressionTypes, EachMessagePayload, KafkaMessage } from 'kafkajs';
import { config } from '../config/config';
import { logger } from '../utils/logger';
import { ConnectionManager } from './connection-manager';
import { safeKafkaHandler, withTimeout } from '../middleware/panic-recovery';
import { kafkaCircuitBreaker, CircuitOpenError } from './circuit-breaker';

interface ProcessedMessage {
    userId: string;
    type: string;
    data: unknown;
    timestamp: string;
}

export class KafkaConsumerService {
    private kafka: Kafka;
    private consumer: Consumer;
    private producer: Producer | null = null;
    private connectionManager: ConnectionManager;
    private isConnected: boolean = false;
    private isShuttingDown: boolean = false;
    private reconnectAttempts: number = 0;

    private readonly topics = [
        'banking.notifications',
        'banking.transfers.completed',
        'banking.fraud.detected',
        'banking.login.attempted',
    ];

    constructor() {
        this.kafka = new Kafka({
            clientId: config.kafka.clientId,
            brokers: config.kafka.brokers,
            logLevel: logLevel.WARN,
            retry: {
                initialRetryTime: config.kafka.reconnect.initialDelayMs,
                retries: config.kafka.reconnect.maxRetries,
                factor: 2,
                maxRetryTime: 30000,
            },
        });

        this.consumer = this.kafka.consumer({
            groupId: config.kafka.groupId,
            sessionTimeout: 30000,
            heartbeatInterval: 3000,
            maxWaitTimeInMs: 5000,
        });

        this.connectionManager = ConnectionManager.getInstance();

        // Setup consumer event handlers
        this.setupEventHandlers();
    }

    /**
     * Setup Kafka consumer event handlers
     */
    private setupEventHandlers(): void {
        this.consumer.on('consumer.connect', () => {
            logger.info('Kafka Consumer connected');
            this.isConnected = true;
            this.reconnectAttempts = 0;
        });

        this.consumer.on('consumer.disconnect', () => {
            logger.warn('Kafka Consumer disconnected');
            this.isConnected = false;
            if (!this.isShuttingDown) {
                this.scheduleReconnect();
            }
        });

        this.consumer.on('consumer.crash', ({ payload }) => {
            logger.error({ error: payload.error }, 'Kafka Consumer crashed');
            this.isConnected = false;
            if (!this.isShuttingDown) {
                this.scheduleReconnect();
            }
        });
    }

    /**
     * Start the Kafka consumer
     */
    public async start(): Promise<void> {
        try {
            await this.connectWithBackoff();
            logger.info('Kafka Consumer started successfully');
        } catch (err) {
            logger.error({ err }, 'Failed to start Kafka Consumer');
            // Schedule reconnection
            this.scheduleReconnect();
        }
    }

    /**
     * Connect with exponential backoff
     */
    private async connectWithBackoff(): Promise<void> {
        const maxRetries = config.kafka.reconnect.maxRetries;
        const initialDelay = config.kafka.reconnect.initialDelayMs;
        let attempt = 0;

        while (attempt < maxRetries && !this.isShuttingDown) {
            try {
                await kafkaCircuitBreaker.execute(async () => {
                    await this.consumer.connect();
                    await this.consumer.subscribe({ topics: this.topics, fromBeginning: false });

                    await this.consumer.run({
                        eachMessage: async (payload) => {
                            await this.processMessage(payload);
                        },
                    });
                });

                this.isConnected = true;
                return;
            } catch (error) {
                attempt++;

                if (error instanceof CircuitOpenError) {
                    logger.warn({
                        retryAfterMs: error.retryAfterMs
                    }, 'Kafka circuit breaker is open');
                    await this.delay(error.retryAfterMs);
                    continue;
                }

                const delay = Math.min(initialDelay * Math.pow(2, attempt), 30000);
                logger.warn({
                    attempt,
                    maxRetries,
                    nextRetryMs: delay,
                    error: error instanceof Error ? error.message : error,
                }, 'Kafka connection failed, retrying');

                if (attempt < maxRetries) {
                    await this.delay(delay);
                }
            }
        }

        if (!this.isConnected && !this.isShuttingDown) {
            throw new Error(`Failed to connect to Kafka after ${maxRetries} attempts`);
        }
    }

    /**
     * Schedule reconnection attempt
     */
    private scheduleReconnect(): void {
        if (this.isShuttingDown) return;

        this.reconnectAttempts++;
        const delay = Math.min(
            config.kafka.reconnect.initialDelayMs * Math.pow(2, this.reconnectAttempts),
            30000
        );

        logger.info({ reconnectAttempts: this.reconnectAttempts, delayMs: delay },
            'Scheduling Kafka reconnection');

        setTimeout(() => {
            if (!this.isShuttingDown) {
                this.connectWithBackoff().catch(err => {
                    logger.error({ err }, 'Kafka reconnection failed');
                });
            }
        }, delay);
    }

    /**
     * Process a Kafka message
     */
    private async processMessage(payload: EachMessagePayload): Promise<void> {
        const { topic, partition, message } = payload;
        const messageId = `${topic}-${partition}-${message.offset}`;

        const result = await safeKafkaHandler(async () => {
            const value = message.value?.toString();
            if (!value) {
                logger.warn({ topic, partition, offset: message.offset },
                    'Received empty message');
                return null;
            }

            // Parse with timeout
            const parsed = await withTimeout(
                Promise.resolve(JSON.parse(value)),
                5000,
                `Parse message ${messageId}`
            );

            return this.handleMessage(topic, parsed);
        }, `processMessage:${messageId}`);

        if (result === null) {
            // Message processing failed, send to DLQ
            await this.sendToDeadLetterQueue(topic, message, 'Processing failed');
        }
    }

    /**
     * Handle a parsed message
     */
    private handleMessage(topic: string, payload: unknown): boolean {
        // Validate payload structure
        if (!payload || typeof payload !== 'object') {
            logger.warn({ topic }, 'Invalid message payload');
            return false;
        }

        const data = payload as Record<string, unknown>;
        const userId = (data.userId || data.account_id || data.user_id) as string | undefined;

        if (!userId) {
            logger.warn({ topic, payload: JSON.stringify(payload).substring(0, 100) },
                'Message received without userId');
            return false;
        }

        // Construct client-facing message
        const clientMessage: ProcessedMessage = {
            userId,
            type: this.mapTopicToEventType(topic),
            timestamp: new Date().toISOString(),
            data: this.sanitizePayload(data),
        };

        this.connectionManager.sendToUser(userId, clientMessage);
        return true;
    }

    /**
     * Map Kafka topic to client event type
     */
    private mapTopicToEventType(topic: string): string {
        const mapping: Record<string, string> = {
            'banking.notifications': 'notification',
            'banking.transfers.completed': 'transfer_completed',
            'banking.fraud.detected': 'fraud_alert',
            'banking.login.attempted': 'login_alert',
        };
        return mapping[topic] || topic.replace('banking.', '').replace('.', '_');
    }

    /**
     * Sanitize payload before sending to client
     */
    private sanitizePayload(data: Record<string, unknown>): Record<string, unknown> {
        const sanitized = { ...data };

        // Remove internal fields
        delete sanitized.internal_correlation_id;
        delete sanitized.kafka_metadata;

        // Mask sensitive fields
        if (sanitized.account_number && typeof sanitized.account_number === 'string') {
            sanitized.account_number = '****' + (sanitized.account_number as string).slice(-4);
        }

        return sanitized;
    }

    /**
     * Send failed message to Dead Letter Queue
     */
    private async sendToDeadLetterQueue(
        originalTopic: string,
        message: KafkaMessage,
        reason: string
    ): Promise<void> {
        try {
            if (!this.producer) {
                this.producer = this.kafka.producer();
                await this.producer.connect();
            }

            await this.producer.send({
                topic: config.kafka.dlqTopic,
                compression: CompressionTypes.GZIP,
                messages: [{
                    key: message.key,
                    value: message.value,
                    headers: {
                        ...message.headers,
                        'x-dlq-original-topic': Buffer.from(originalTopic),
                        'x-dlq-reason': Buffer.from(reason),
                        'x-dlq-timestamp': Buffer.from(new Date().toISOString()),
                        'x-dlq-original-offset': Buffer.from(message.offset),
                    },
                }],
            });

            logger.warn({ originalTopic, reason }, 'Message sent to DLQ');
        } catch (error) {
            logger.error({ error, originalTopic }, 'Failed to send message to DLQ');
        }
    }

    /**
     * Helper delay function
     */
    private delay(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Check if consumer is connected
     */
    public isHealthy(): boolean {
        return this.isConnected;
    }

    /**
     * Get Kafka client for health checks
     */
    public getKafkaClient(): Kafka {
        return this.kafka;
    }

    /**
     * Disconnect consumer gracefully
     */
    public async disconnect(): Promise<void> {
        this.isShuttingDown = true;

        try {
            if (this.producer) {
                await this.producer.disconnect();
            }
            await this.consumer.disconnect();
            logger.info('Kafka Consumer disconnected');
        } catch (error) {
            logger.error({ error }, 'Error disconnecting Kafka Consumer');
        }
    }
}
