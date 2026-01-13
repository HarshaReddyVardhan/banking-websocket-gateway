// Test setup file
import { jest } from '@jest/globals';

// Set test environment
process.env.NODE_ENV = 'test';
process.env.JWT_SECRET = 'test-secret-key';
process.env.JWT_ISSUER = 'test-issuer';
process.env.JWT_AUDIENCE = 'test-audience';
process.env.REDIS_URL = 'redis://localhost:6379';
process.env.KAFKA_BROKERS = 'localhost:9092';
process.env.LOG_LEVEL = 'error'; // Reduce log noise in tests

// Mock pino logger
jest.mock('../src/utils/logger', () => ({
    logger: {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
        fatal: jest.fn(),
        trace: jest.fn(),
    },
}));

// Global test utilities
global.testUtils = {
    createMockWebSocket: () => ({
        readyState: 1, // OPEN
        send: jest.fn(),
        close: jest.fn(),
        terminate: jest.fn(),
        ping: jest.fn(),
        on: jest.fn(),
    }),

    createMockRedis: () => ({
        get: jest.fn(),
        set: jest.fn(),
        setex: jest.fn(),
        del: jest.fn(),
        incr: jest.fn(),
        expire: jest.fn(),
        eval: jest.fn(),
        publish: jest.fn(),
        subscribe: jest.fn(),
        on: jest.fn(),
        ping: jest.fn().mockResolvedValue('PONG'),
        hgetall: jest.fn(),
        quit: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn(),
        status: 'ready',
    }),

    createMockRequest: (overrides = {}) => ({
        ip: '127.0.0.1',
        socket: { remoteAddress: '127.0.0.1' },
        headers: {
            'user-agent': 'test-agent',
            origin: 'http://localhost:3000',
        },
        method: 'GET',
        path: '/test',
        ...overrides,
    }),

    createMockResponse: () => {
        const res: any = {
            status: jest.fn().mockReturnThis(),
            json: jest.fn().mockReturnThis(),
            send: jest.fn().mockReturnThis(),
            end: jest.fn().mockReturnThis(),
            set: jest.fn().mockReturnThis(),
            setHeader: jest.fn().mockReturnThis(),
        };
        return res;
    },

    delay: (ms: number) => new Promise(resolve => setTimeout(resolve, ms)),

    generateJwt: (payload: object, expiresIn = '1h') => {
        const jwt = require('jsonwebtoken');
        return jwt.sign(payload, process.env.JWT_SECRET, {
            issuer: process.env.JWT_ISSUER,
            audience: process.env.JWT_AUDIENCE,
            expiresIn,
        });
    },
};

// Extend Jest matchers
declare global {
    var testUtils: {
        createMockWebSocket: () => any;
        createMockRedis: () => any;
        createMockRequest: (overrides?: object) => any;
        createMockResponse: () => any;
        delay: (ms: number) => Promise<void>;
        generateJwt: (payload: object, expiresIn?: string) => string;
    };
}

// Cleanup after all tests
afterAll(async () => {
    // Give time for any async operations to complete
    await new Promise(resolve => setTimeout(resolve, 100));
});
