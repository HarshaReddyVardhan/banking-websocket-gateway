import http from 'http';
import { WebSocket } from 'ws';
import express from 'express';
import jwt from 'jsonwebtoken';

// Test configuration
const TEST_PORT = 9999;
const JWT_SECRET = 'test-secret-key';
const JWT_ISSUER = 'test-issuer';
const JWT_AUDIENCE = 'test-audience';

// Mock dependencies
jest.mock('kafkajs', () => ({
    Kafka: jest.fn().mockImplementation(() => ({
        consumer: jest.fn().mockReturnValue({
            connect: jest.fn(),
            subscribe: jest.fn(),
            run: jest.fn(),
            disconnect: jest.fn(),
            on: jest.fn(),
        }),
        producer: jest.fn().mockReturnValue({
            connect: jest.fn(),
            send: jest.fn(),
            disconnect: jest.fn(),
        }),
        admin: jest.fn().mockReturnValue({
            connect: jest.fn(),
            describeCluster: jest.fn().mockResolvedValue({ brokers: [{ nodeId: 1 }] }),
            disconnect: jest.fn(),
        }),
    })),
    logLevel: { WARN: 4 },
    CompressionTypes: { GZIP: 1 },
}));

jest.mock('ioredis', () => {
    return jest.fn().mockImplementation(() => ({
        on: jest.fn(),
        get: jest.fn().mockResolvedValue(null),
        set: jest.fn().mockResolvedValue('OK'),
        setex: jest.fn().mockResolvedValue('OK'),
        del: jest.fn().mockResolvedValue(1),
        incr: jest.fn().mockResolvedValue(1),
        expire: jest.fn().mockResolvedValue(1),
        eval: jest.fn().mockResolvedValue([1, 9, 0]),
        publish: jest.fn().mockResolvedValue(1),
        subscribe: jest.fn().mockResolvedValue(undefined),
        ping: jest.fn().mockResolvedValue('PONG'),
        hgetall: jest.fn().mockResolvedValue({}),
        hset: jest.fn().mockResolvedValue(1),
        quit: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn(),
        status: 'ready',
    }));
});

// Set environment before importing modules
process.env.NODE_ENV = 'test';
process.env.PORT = TEST_PORT.toString();
process.env.JWT_SECRET = JWT_SECRET;
process.env.JWT_ISSUER = JWT_ISSUER;
process.env.JWT_AUDIENCE = JWT_AUDIENCE;
process.env.LOG_LEVEL = 'error';
process.env.CORS_ALLOWED_ORIGINS = 'http://localhost:3000';

describe('WebSocket Gateway Integration', () => {
    let server: http.Server;
    let app: express.Application;

    function generateValidToken(userId: string, expiresIn = '1h'): string {
        return jwt.sign(
            { sub: userId, type: 'access' },
            JWT_SECRET,
            { issuer: JWT_ISSUER, audience: JWT_AUDIENCE, expiresIn }
        );
    }

    function generateInvalidToken(userId: string): string {
        return jwt.sign(
            { sub: userId },
            'wrong-secret',
            { issuer: JWT_ISSUER, audience: JWT_AUDIENCE }
        );
    }

    beforeAll(async () => {
        // Import after mocks are set up
        const { WebSocketGateway } = await import('../../src/services/websocket-gateway');

        app = express();
        server = http.createServer(app);

        // Initialize WebSocket Gateway
        new WebSocketGateway(server);

        // Start server
        await new Promise<void>((resolve) => {
            server.listen(TEST_PORT, () => resolve());
        });
    });

    afterAll(async () => {
        await new Promise<void>((resolve) => {
            server.close(() => resolve());
        });
    });

    describe('Connection Authentication', () => {
        it('should reject connection without token', (done) => {
            const ws = new WebSocket(`ws://localhost:${TEST_PORT}`);

            ws.on('error', (error) => {
                expect(error).toBeDefined();
                done();
            });

            ws.on('unexpected-response', (req, res) => {
                expect(res.statusCode).toBe(401);
                done();
            });
        });

        it('should reject connection with invalid token', (done) => {
            const token = generateInvalidToken('user-1');
            const ws = new WebSocket(`ws://localhost:${TEST_PORT}?token=${token}`);

            ws.on('unexpected-response', (req, res) => {
                expect(res.statusCode).toBe(401);
                done();
            });

            ws.on('error', () => {
                // Expected
                done();
            });
        });

        it('should accept connection with valid token', (done) => {
            const token = generateValidToken('user-1');
            const ws = new WebSocket(`ws://localhost:${TEST_PORT}?token=${token}`);

            ws.on('open', () => {
                expect(ws.readyState).toBe(WebSocket.OPEN);
                ws.close();
                done();
            });

            ws.on('error', (error) => {
                done(error);
            });
        });
    });

    describe('Message Handling', () => {
        it('should respond to ping messages', (done) => {
            const token = generateValidToken('user-2');
            const ws = new WebSocket(`ws://localhost:${TEST_PORT}?token=${token}`);

            ws.on('open', () => {
                ws.send(JSON.stringify({ type: 'ping', requestId: 'test-123' }));
            });

            ws.on('message', (data) => {
                const message = JSON.parse(data.toString());
                if (message.type === 'pong') {
                    expect(message.requestId).toBe('test-123');
                    ws.close();
                    done();
                }
            });

            ws.on('error', (error) => {
                done(error);
            });
        });

        it('should reject invalid JSON messages', (done) => {
            const token = generateValidToken('user-3');
            const ws = new WebSocket(`ws://localhost:${TEST_PORT}?token=${token}`);

            ws.on('open', () => {
                ws.send('not valid json');
            });

            ws.on('message', (data) => {
                const message = JSON.parse(data.toString());
                if (message.type === 'error') {
                    expect(message.error).toBe('Invalid JSON');
                    ws.close();
                    done();
                }
            });

            ws.on('error', (error) => {
                done(error);
            });
        });

        it('should reject messages with invalid schema', (done) => {
            const token = generateValidToken('user-4');
            const ws = new WebSocket(`ws://localhost:${TEST_PORT}?token=${token}`);

            ws.on('open', () => {
                ws.send(JSON.stringify({ invalid: 'schema' }));
            });

            ws.on('message', (data) => {
                const message = JSON.parse(data.toString());
                if (message.type === 'error') {
                    expect(message.error).toBe('Invalid message format');
                    ws.close();
                    done();
                }
            });

            ws.on('error', (error) => {
                done(error);
            });
        });
    });

    describe('Connection Limits', () => {
        it('should allow multiple connections from same user up to limit', async () => {
            const token = generateValidToken('user-multi');
            const connections: WebSocket[] = [];

            // Create 5 connections (default max)
            for (let i = 0; i < 5; i++) {
                const ws = new WebSocket(`ws://localhost:${TEST_PORT}?token=${token}`);
                await new Promise<void>((resolve, reject) => {
                    ws.on('open', () => {
                        connections.push(ws);
                        resolve();
                    });
                    ws.on('error', reject);
                });
            }

            expect(connections.length).toBe(5);

            // Clean up
            connections.forEach(ws => ws.close());
        });
    });
});

describe('HTTP Endpoints Integration', () => {
    let server: http.Server;
    let baseUrl: string;

    beforeAll(async () => {
        // Import after environment is set
        const { healthChecker } = await import('../../src/services/health-checker');

        const app = express();

        app.get('/health', (req, res) => {
            res.json({ status: 'ok', uptime: 1000 });
        });

        app.get('/ready', async (req, res) => {
            const health = await healthChecker.checkAll();
            res.status(health.healthy ? 200 : 503).json(health);
        });

        server = http.createServer(app);
        const port = 9998;
        baseUrl = `http://localhost:${port}`;

        await new Promise<void>((resolve) => {
            server.listen(port, () => resolve());
        });
    });

    afterAll(async () => {
        await new Promise<void>((resolve) => {
            server.close(() => resolve());
        });
    });

    it('should return 200 for health check', async () => {
        const response = await fetch(`${baseUrl}/health`);
        expect(response.status).toBe(200);

        const data = await response.json();
        expect(data.status).toBe('ok');
    });

    it('should return health status for ready check', async () => {
        const response = await fetch(`${baseUrl}/ready`);
        const data = await response.json();

        expect(data).toHaveProperty('healthy');
        expect(data).toHaveProperty('components');
    });
});
