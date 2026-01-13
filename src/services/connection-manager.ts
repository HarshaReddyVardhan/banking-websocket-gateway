import { WebSocket } from 'ws';
import { v4 as uuidv4 } from 'uuid';
import { logger } from '../utils/logger';
import { RedisService } from './redis';
import { config } from '../config/config';
import { auditLogger } from '../middleware/audit-logger';

interface Connection {
    id: string;
    ws: WebSocket;
    userId: string;
    ip: string;
    connectedAt: Date;
    lastActivity: Date;
    isAlive: boolean;
}

export class ConnectionManager {
    private static instance: ConnectionManager;
    private connections: Map<string, Connection[]> = new Map(); // userId -> Connection[]
    private connectionById: Map<string, Connection> = new Map(); // connectionId -> Connection
    private redisService: RedisService;
    private heartbeatInterval: NodeJS.Timeout | null = null;
    private cleanupInterval: NodeJS.Timeout | null = null;
    private isShuttingDown: boolean = false;

    private constructor() {
        this.redisService = RedisService.getInstance();
        this.setupRedisSubscription();
        this.startHeartbeat();
        this.startIdleCleanup();
    }

    public static getInstance(): ConnectionManager {
        if (!ConnectionManager.instance) {
            ConnectionManager.instance = new ConnectionManager();
        }
        return ConnectionManager.instance;
    }

    private setupRedisSubscription() {
        this.redisService.subscribe('broadcast', (message) => {
            try {
                const payload = JSON.parse(message);
                const { userId, data, senderId } = payload;

                // Avoid processing our own broadcasts
                if (senderId === config.kafka.clientId) {
                    return;
                }

                this.sendToLocalUser(userId, data);
            } catch (err) {
                logger.error({ err }, 'Failed to parse broadcast message');
            }
        });
    }

    /**
     * Start heartbeat mechanism to detect dead connections
     */
    private startHeartbeat(): void {
        this.heartbeatInterval = setInterval(() => {
            if (this.isShuttingDown) return;

            this.connectionById.forEach((connection) => {
                if (!connection.isAlive) {
                    // Connection didn't respond to last ping
                    logger.warn({
                        userId: connection.userId,
                        connectionId: connection.id
                    }, 'Connection failed heartbeat, terminating');

                    connection.ws.terminate();
                    return;
                }

                // Mark as not alive and send ping
                connection.isAlive = false;
                connection.ws.ping();
            });
        }, config.websocket.heartbeatIntervalMs);

        logger.info({ intervalMs: config.websocket.heartbeatIntervalMs }, 'Heartbeat started');
    }

    /**
     * Start cleanup of idle connections
     */
    private startIdleCleanup(): void {
        this.cleanupInterval = setInterval(() => {
            if (this.isShuttingDown) return;

            const now = Date.now();
            const idleThreshold = config.websocket.idleTimeoutMs;

            this.connectionById.forEach((connection) => {
                const idleTime = now - connection.lastActivity.getTime();
                if (idleTime > idleThreshold) {
                    logger.info({
                        userId: connection.userId,
                        connectionId: connection.id,
                        idleTimeMs: idleTime,
                    }, 'Closing idle connection');

                    connection.ws.close(1000, 'Idle timeout');
                }
            });
        }, 60000); // Check every minute

        logger.info({ timeoutMs: config.websocket.idleTimeoutMs }, 'Idle cleanup started');
    }

    /**
     * Check if user can add a new connection
     */
    public canAddConnection(userId: string): boolean {
        const userConns = this.connections.get(userId);
        const currentCount = userConns?.length || 0;
        return currentCount < config.rateLimit.maxConnectionsPerUser;
    }

    /**
     * Add a new connection
     */
    public addConnection(userId: string, ws: WebSocket, ip: string): string {
        const connectionId = uuidv4();
        const now = new Date();

        const connection: Connection = {
            id: connectionId,
            ws,
            userId,
            ip,
            connectedAt: now,
            lastActivity: now,
            isAlive: true,
        };

        if (!this.connections.has(userId)) {
            this.connections.set(userId, []);
        }
        this.connections.get(userId)!.push(connection);
        this.connectionById.set(connectionId, connection);

        logger.info({ userId, connectionId, ip }, 'Connection established');

        // Setup event handlers
        ws.on('close', (code, reason) => {
            const durationMs = Date.now() - connection.connectedAt.getTime();
            auditLogger.logConnectionClosed({
                ip,
                userId,
                connectionId,
                reason: reason?.toString() || `Code: ${code}`,
                durationMs,
            });
            this.removeConnection(userId, connectionId);
        });

        ws.on('pong', () => {
            connection.isAlive = true;
            connection.lastActivity = new Date();
        });

        ws.on('message', () => {
            connection.lastActivity = new Date();
        });

        return connectionId;
    }

    /**
     * Remove a connection
     */
    public removeConnection(userId: string, connectionId: string): void {
        const userConns = this.connections.get(userId);
        if (!userConns) return;

        const updatedConns = userConns.filter(c => c.id !== connectionId);

        if (updatedConns.length === 0) {
            this.connections.delete(userId);
        } else {
            this.connections.set(userId, updatedConns);
        }

        this.connectionById.delete(connectionId);
        logger.info({ userId, connectionId }, 'Connection closed');
    }

    /**
     * Send message to a specific user
     */
    public sendToUser(userId: string, message: any): void {
        this.sendToLocalUser(userId, message);

        // Broadcast to other gateways
        this.redisService.publish('broadcast', JSON.stringify({
            userId,
            data: message,
            senderId: config.kafka.clientId,
        }));
    }

    /**
     * Send to local connections only
     */
    private sendToLocalUser(userId: string, message: any): boolean {
        const userConns = this.connections.get(userId);
        if (!userConns || userConns.length === 0) return false;

        const payload = typeof message === 'string' ? message : JSON.stringify(message);
        let sent = false;

        userConns.forEach(conn => {
            if (conn.ws.readyState === WebSocket.OPEN) {
                try {
                    conn.ws.send(payload);
                    sent = true;
                } catch (error) {
                    logger.error({ error, userId, connectionId: conn.id }, 'Failed to send message');
                }
            }
        });

        return sent;
    }

    /**
     * Broadcast to all connected users
     */
    public broadcast(message: any): void {
        const payload = typeof message === 'string' ? message : JSON.stringify(message);

        this.connectionById.forEach((connection) => {
            if (connection.ws.readyState === WebSocket.OPEN) {
                try {
                    connection.ws.send(payload);
                } catch (error) {
                    logger.error({ error, connectionId: connection.id }, 'Failed to broadcast');
                }
            }
        });
    }

    /**
     * Get connection metrics
     */
    public getMetrics(): {
        totalConnections: number;
        activeUsers: number;
        connectionsByUser: Map<string, number>;
    } {
        const connectionsByUser = new Map<string, number>();
        this.connections.forEach((conns, userId) => {
            connectionsByUser.set(userId, conns.length);
        });

        return {
            totalConnections: this.connectionById.size,
            activeUsers: this.connections.size,
            connectionsByUser,
        };
    }

    /**
     * Gracefully drain all connections for shutdown
     */
    public async drainConnections(): Promise<void> {
        this.isShuttingDown = true;

        // Stop intervals
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
            this.heartbeatInterval = null;
        }
        if (this.cleanupInterval) {
            clearInterval(this.cleanupInterval);
            this.cleanupInterval = null;
        }

        // Notify all clients
        const closeMessage = JSON.stringify({
            type: 'server_shutdown',
            message: 'Server is shutting down. Please reconnect.',
        });

        const closePromises: Promise<void>[] = [];

        this.connectionById.forEach((connection) => {
            closePromises.push(new Promise<void>((resolve) => {
                try {
                    connection.ws.send(closeMessage);
                    connection.ws.close(1001, 'Server shutdown');
                } catch (error) {
                    logger.error({ error, connectionId: connection.id }, 'Error closing connection');
                }
                // Wait a bit for close to complete
                setTimeout(resolve, 100);
            }));
        });

        await Promise.all(closePromises);

        // Force terminate any remaining
        this.connectionById.forEach((connection) => {
            try {
                connection.ws.terminate();
            } catch { }
        });

        this.connections.clear();
        this.connectionById.clear();

        logger.info('All connections drained');
    }

    /**
     * Get active connection count (for health checks)
     */
    public getConnectionCount(): number {
        return this.connectionById.size;
    }
}
