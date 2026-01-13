import { Kafka } from 'kafkajs';

// Mock all dependencies before importing
jest.mock('kafkajs', () => {
    const mockConsumer = {
        connect: jest.fn().mockResolvedValue(undefined),
        subscribe: jest.fn().mockResolvedValue(undefined),
        run: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        on: jest.fn(),
    };

    const mockProducer = {
        connect: jest.fn().mockResolvedValue(undefined),
        send: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
    };

    return {
        Kafka: jest.fn().mockImplementation(() => ({
            consumer: jest.fn().mockReturnValue(mockConsumer),
            producer: jest.fn().mockReturnValue(mockProducer),
            admin: jest.fn().mockReturnValue({
                connect: jest.fn().mockResolvedValue(undefined),
                describeCluster: jest.fn().mockResolvedValue({ brokers: [{ nodeId: 1 }] }),
                disconnect: jest.fn().mockResolvedValue(undefined),
            }),
        })),
        logLevel: { WARN: 4 },
        CompressionTypes: { GZIP: 1 },
    };
});

jest.mock('../../src/services/connection-manager', () => ({
    ConnectionManager: {
        getInstance: jest.fn().mockReturnValue({
            sendToUser: jest.fn(),
        }),
    },
}));

jest.mock('../../src/services/circuit-breaker', () => ({
    kafkaCircuitBreaker: {
        execute: jest.fn().mockImplementation((fn) => fn()),
    },
    CircuitOpenError: class CircuitOpenError extends Error {
        retryAfterMs: number;
        constructor(name: string, retryAfterMs: number) {
            super(`Circuit breaker '${name}' is open`);
            this.retryAfterMs = retryAfterMs;
        }
    },
}));

// Set environment before importing
process.env.NODE_ENV = 'test';
process.env.LOG_LEVEL = 'error';
process.env.KAFKA_BROKERS = 'localhost:9092';

import { KafkaConsumerService } from '../../src/services/kafka-consumer';
import { ConnectionManager } from '../../src/services/connection-manager';

describe('KafkaConsumerService', () => {
    let consumerService: KafkaConsumerService;
    let mockConnectionManager: any;

    beforeEach(() => {
        jest.clearAllMocks();
        mockConnectionManager = ConnectionManager.getInstance();
        consumerService = new KafkaConsumerService();
    });

    afterEach(async () => {
        await consumerService.disconnect();
    });

    describe('Constructor', () => {
        it('should create Kafka client with correct config', () => {
            expect(Kafka).toHaveBeenCalledWith(expect.objectContaining({
                clientId: expect.any(String),
                brokers: expect.any(Array),
            }));
        });
    });

    describe('start', () => {
        it('should connect consumer and subscribe to topics', async () => {
            await consumerService.start();

            const kafkaInstance = (Kafka as jest.Mock).mock.results[0].value;
            const consumer = kafkaInstance.consumer();

            expect(consumer.connect).toHaveBeenCalled();
            expect(consumer.subscribe).toHaveBeenCalledWith(
                expect.objectContaining({
                    topics: expect.arrayContaining([
                        'banking.notifications',
                        'banking.transfers.completed',
                        'banking.fraud.detected',
                        'banking.login.attempted',
                    ]),
                })
            );
        });

        it('should start consumer run loop', async () => {
            await consumerService.start();

            const kafkaInstance = (Kafka as jest.Mock).mock.results[0].value;
            const consumer = kafkaInstance.consumer();

            expect(consumer.run).toHaveBeenCalledWith(
                expect.objectContaining({
                    eachMessage: expect.any(Function),
                })
            );
        });
    });

    describe('isHealthy', () => {
        it('should return false before start', () => {
            expect(consumerService.isHealthy()).toBe(false);
        });

        it('should return true after successful start', async () => {
            await consumerService.start();
            expect(consumerService.isHealthy()).toBe(true);
        });
    });

    describe('disconnect', () => {
        it('should disconnect consumer', async () => {
            await consumerService.start();
            await consumerService.disconnect();

            const kafkaInstance = (Kafka as jest.Mock).mock.results[0].value;
            const consumer = kafkaInstance.consumer();

            expect(consumer.disconnect).toHaveBeenCalled();
        });
    });

    describe('getKafkaClient', () => {
        it('should return kafka client', () => {
            const client = consumerService.getKafkaClient();
            expect(client).toBeDefined();
        });
    });
});

describe('Message Handling', () => {
    let consumerService: KafkaConsumerService;
    let eachMessageHandler: Function;

    beforeEach(async () => {
        jest.clearAllMocks();
        consumerService = new KafkaConsumerService();

        // Start and capture the eachMessage handler
        await consumerService.start();
        const kafkaInstance = (Kafka as jest.Mock).mock.results[0].value;
        const consumer = kafkaInstance.consumer();
        eachMessageHandler = consumer.run.mock.calls[0][0].eachMessage;
    });

    afterEach(async () => {
        await consumerService.disconnect();
    });

    it('should process valid messages and send to user', async () => {
        const mockConnectionManager = ConnectionManager.getInstance();

        const mockPayload = {
            topic: 'banking.notifications',
            partition: 0,
            message: {
                key: Buffer.from('key'),
                value: Buffer.from(JSON.stringify({ userId: 'user-123', data: 'test' })),
                headers: {},
                timestamp: Date.now().toString(),
                offset: '1',
            },
        };

        await eachMessageHandler(mockPayload);

        expect(mockConnectionManager.sendToUser).toHaveBeenCalledWith(
            'user-123',
            expect.objectContaining({
                type: expect.any(String),
                data: expect.any(Object),
            })
        );
    });

    it('should handle messages with account_id instead of userId', async () => {
        const mockConnectionManager = ConnectionManager.getInstance();

        const mockPayload = {
            topic: 'banking.transfers.completed',
            partition: 0,
            message: {
                key: Buffer.from('key'),
                value: Buffer.from(JSON.stringify({ account_id: 'account-456', amount: 100 })),
                headers: {},
                timestamp: Date.now().toString(),
                offset: '2',
            },
        };

        await eachMessageHandler(mockPayload);

        expect(mockConnectionManager.sendToUser).toHaveBeenCalledWith(
            'account-456',
            expect.any(Object)
        );
    });

    it('should skip messages without userId', async () => {
        const mockConnectionManager = ConnectionManager.getInstance();

        const mockPayload = {
            topic: 'banking.notifications',
            partition: 0,
            message: {
                key: Buffer.from('key'),
                value: Buffer.from(JSON.stringify({ data: 'no user id' })),
                headers: {},
                timestamp: Date.now().toString(),
                offset: '3',
            },
        };

        await eachMessageHandler(mockPayload);

        expect(mockConnectionManager.sendToUser).not.toHaveBeenCalled();
    });

    it('should handle empty message value gracefully', async () => {
        const mockPayload = {
            topic: 'banking.notifications',
            partition: 0,
            message: {
                key: Buffer.from('key'),
                value: null,
                headers: {},
                timestamp: Date.now().toString(),
                offset: '4',
            },
        };

        // Should not throw
        await expect(eachMessageHandler(mockPayload)).resolves.not.toThrow();
    });

    it('should handle invalid JSON gracefully', async () => {
        const mockPayload = {
            topic: 'banking.notifications',
            partition: 0,
            message: {
                key: Buffer.from('key'),
                value: Buffer.from('not valid json'),
                headers: {},
                timestamp: Date.now().toString(),
                offset: '5',
            },
        };

        // Should not throw
        await expect(eachMessageHandler(mockPayload)).resolves.not.toThrow();
    });
});
