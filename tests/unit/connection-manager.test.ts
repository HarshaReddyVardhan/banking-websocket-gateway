import { WebSocket } from 'ws';

// Mock dependencies before importing
jest.mock('../../src/services/redis', () => ({
    RedisService: {
        getInstance: jest.fn().mockReturnValue({
            subscribe: jest.fn(),
            publish: jest.fn(),
            getPubClient: jest.fn().mockReturnValue(global.testUtils.createMockRedis()),
        }),
    },
}));

jest.mock('../../src/middleware/rate-limiter', () => ({
    RateLimiter: {
        getInstance: jest.fn().mockReturnValue({
            setRedisClient: jest.fn(),
        }),
    },
}));

import { ConnectionManager } from '../../src/services/connection-manager';

describe('ConnectionManager', () => {
    let manager: ConnectionManager;
    let mockWs: any;

    beforeEach(() => {
        // Reset singleton
        (ConnectionManager as any).instance = undefined;
        manager = ConnectionManager.getInstance();
        mockWs = {
            readyState: WebSocket.OPEN,
            send: jest.fn(),
            close: jest.fn(),
            terminate: jest.fn(),
            ping: jest.fn(),
            on: jest.fn(),
        };
    });

    afterEach(() => {
        // Clear intervals
        (manager as any).heartbeatInterval && clearInterval((manager as any).heartbeatInterval);
        (manager as any).cleanupInterval && clearInterval((manager as any).cleanupInterval);
    });

    describe('Singleton', () => {
        it('should return same instance', () => {
            const instance1 = ConnectionManager.getInstance();
            const instance2 = ConnectionManager.getInstance();
            expect(instance1).toBe(instance2);
        });
    });

    describe('addConnection', () => {
        it('should add a connection and return connection ID', () => {
            const connectionId = manager.addConnection('user-1', mockWs, '192.168.1.1');

            expect(connectionId).toBeDefined();
            expect(typeof connectionId).toBe('string');
            expect(connectionId.length).toBe(36); // UUID format
        });

        it('should register event handlers on WebSocket', () => {
            manager.addConnection('user-1', mockWs, '192.168.1.1');

            expect(mockWs.on).toHaveBeenCalledWith('close', expect.any(Function));
            expect(mockWs.on).toHaveBeenCalledWith('pong', expect.any(Function));
            expect(mockWs.on).toHaveBeenCalledWith('message', expect.any(Function));
        });

        it('should track multiple connections per user', () => {
            const mockWs2 = { ...mockWs, on: jest.fn() };

            manager.addConnection('user-1', mockWs, '192.168.1.1');
            manager.addConnection('user-1', mockWs2, '192.168.1.2');

            const metrics = manager.getMetrics();
            expect(metrics.activeUsers).toBe(1);
            expect(metrics.totalConnections).toBe(2);
        });
    });

    describe('canAddConnection', () => {
        it('should allow connections under the limit', () => {
            expect(manager.canAddConnection('new-user')).toBe(true);
        });

        it('should reject when max connections reached', () => {
            // Add max connections (default 5)
            for (let i = 0; i < 5; i++) {
                const ws = { ...mockWs, on: jest.fn() };
                manager.addConnection('max-user', ws, '192.168.1.1');
            }

            expect(manager.canAddConnection('max-user')).toBe(false);
        });
    });

    describe('removeConnection', () => {
        it('should remove a connection', () => {
            const connectionId = manager.addConnection('user-1', mockWs, '192.168.1.1');

            manager.removeConnection('user-1', connectionId);

            const metrics = manager.getMetrics();
            expect(metrics.totalConnections).toBe(0);
        });

        it('should delete user entry when no connections remain', () => {
            const connectionId = manager.addConnection('user-1', mockWs, '192.168.1.1');

            manager.removeConnection('user-1', connectionId);

            const metrics = manager.getMetrics();
            expect(metrics.activeUsers).toBe(0);
        });

        it('should handle non-existent connection gracefully', () => {
            expect(() => {
                manager.removeConnection('non-existent', 'fake-id');
            }).not.toThrow();
        });
    });

    describe('sendToUser', () => {
        it('should send message to all user connections', () => {
            const mockWs2 = { ...mockWs, on: jest.fn(), send: jest.fn() };

            manager.addConnection('user-1', mockWs, '192.168.1.1');
            manager.addConnection('user-1', mockWs2, '192.168.1.2');

            manager.sendToUser('user-1', { type: 'test', data: 'hello' });

            expect(mockWs.send).toHaveBeenCalledWith(
                JSON.stringify({ type: 'test', data: 'hello' })
            );
            expect(mockWs2.send).toHaveBeenCalledWith(
                JSON.stringify({ type: 'test', data: 'hello' })
            );
        });

        it('should not send to closed connections', () => {
            const closedWs = {
                ...mockWs,
                readyState: WebSocket.CLOSED,
                on: jest.fn(),
            };

            manager.addConnection('user-1', closedWs, '192.168.1.1');
            manager.sendToUser('user-1', { type: 'test' });

            expect(closedWs.send).not.toHaveBeenCalled();
        });
    });

    describe('broadcast', () => {
        it('should send message to all connections', () => {
            const mockWs2 = { ...mockWs, on: jest.fn(), send: jest.fn() };

            manager.addConnection('user-1', mockWs, '192.168.1.1');
            manager.addConnection('user-2', mockWs2, '192.168.1.2');

            manager.broadcast({ type: 'broadcast', message: 'hello all' });

            expect(mockWs.send).toHaveBeenCalled();
            expect(mockWs2.send).toHaveBeenCalled();
        });
    });

    describe('getMetrics', () => {
        it('should return correct metrics', () => {
            const mockWs2 = { ...mockWs, on: jest.fn() };
            const mockWs3 = { ...mockWs, on: jest.fn() };

            manager.addConnection('user-1', mockWs, '192.168.1.1');
            manager.addConnection('user-1', mockWs2, '192.168.1.2');
            manager.addConnection('user-2', mockWs3, '192.168.1.3');

            const metrics = manager.getMetrics();

            expect(metrics.totalConnections).toBe(3);
            expect(metrics.activeUsers).toBe(2);
            expect(metrics.connectionsByUser.get('user-1')).toBe(2);
            expect(metrics.connectionsByUser.get('user-2')).toBe(1);
        });
    });

    describe('getConnectionCount', () => {
        it('should return total connection count', () => {
            manager.addConnection('user-1', mockWs, '192.168.1.1');
            expect(manager.getConnectionCount()).toBe(1);
        });
    });

    describe('drainConnections', () => {
        it('should close all connections gracefully', async () => {
            const mockWs2 = { ...mockWs, on: jest.fn() };

            manager.addConnection('user-1', mockWs, '192.168.1.1');
            manager.addConnection('user-2', mockWs2, '192.168.1.2');

            await manager.drainConnections();

            expect(mockWs.send).toHaveBeenCalledWith(
                expect.stringContaining('server_shutdown')
            );
            expect(mockWs.close).toHaveBeenCalled();
            expect(mockWs2.close).toHaveBeenCalled();
        });

        it('should clear all connections after drain', async () => {
            manager.addConnection('user-1', mockWs, '192.168.1.1');

            await manager.drainConnections();

            const metrics = manager.getMetrics();
            expect(metrics.totalConnections).toBe(0);
        });
    });
});
