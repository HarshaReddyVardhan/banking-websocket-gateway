import pino from 'pino';
import { config } from '../config/config';

export const logger = pino({
    level: config.logging.level,
    transport: {
        target: 'pino-pretty',
        options: {
            colorize: true,
            translateTime: 'SYS:standard',
            ignore: 'pid,hostname',
        },
    },
});
