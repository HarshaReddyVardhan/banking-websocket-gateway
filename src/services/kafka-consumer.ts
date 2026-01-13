import { Kafka, Consumer } from 'kafkajs';
import { config } from '../config/config';
import { logger } from '../utils/logger';
import { ConnectionManager } from './connection-manager';

export class KafkaConsumerService {
    private kafka: Kafka;
    private consumer: Consumer;
    private connectionManager: ConnectionManager;

    constructor() {
        this.kafka = new Kafka({
            clientId: config.kafka.clientId,
            brokers: config.kafka.brokers,
        });
        this.consumer = this.kafka.consumer({ groupId: config.kafka.groupId });
        this.connectionManager = ConnectionManager.getInstance();
    }

    public async start() {
        try {
            await this.consumer.connect();
            logger.info('Kafka Consumer connected');

            // Subscribe to topics
            const topics = [
                'banking.notifications',
                'banking.transfers.completed',
                'banking.fraud.detected',
                'banking.login.attempted'
            ];

            await this.consumer.subscribe({ topics, fromBeginning: false });

            await this.consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    try {
                        const value = message.value?.toString();
                        if (!value) return;

                        const payload = JSON.parse(value);
                        // Payload should contain { userId: string, type: string, data: any }
                        // Adjust based on actual event schema from other services.
                        // Assuming a standard envelope or extracting userId from the event.

                        this.handleMessage(topic, payload);
                    } catch (err) {
                        logger.error({ err, topic }, 'Failed to process Kafka message');
                    }
                },
            });
        } catch (err) {
            logger.error({ err }, 'Failed to start Kafka Consumer');
            // Retry logic could be added here
        }
    }

    private handleMessage(topic: string, payload: any) {
        const userId = payload.userId || payload.account_id; // Mapping based on event
        if (!userId) {
            logger.warn({ topic, payload }, 'Message received without userId');
            return;
        }

        // Construct client-facing message
        const clientMessage = {
            type: topic,
            timestamp: new Date().toISOString(),
            data: payload
        };

        this.connectionManager.sendToUser(userId, clientMessage);
    }

    public async disconnect() {
        await this.consumer.disconnect();
    }
}
