import Redis from 'ioredis';
import { config } from '../config/config';
import { logger } from '../utils/logger';

export class RedisService {
    private static instance: RedisService;
    private pubClient: Redis;
    private subClient: Redis;

    private constructor() {
        this.pubClient = new Redis(config.redis.url);
        this.subClient = new Redis(config.redis.url);

        this.pubClient.on('error', (err) => logger.error({ err }, 'Redis Pub Client Error'));
        this.subClient.on('error', (err) => logger.error({ err }, 'Redis Sub Client Error'));
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

    public async publish(channel: string, message: string): Promise<number> {
        return this.pubClient.publish(channel, message);
    }

    public async subscribe(channel: string, callback: (message: string) => void): Promise<void> {
        await this.subClient.subscribe(channel);
        this.subClient.on('message', (chn, msg) => {
            if (chn === channel) {
                callback(msg);
            }
        });
    }
}
