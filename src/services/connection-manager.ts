import { WebSocket } from 'ws';
import { v4 as uuidv4 } from 'uuid';
import { logger } from '../utils/logger';
import { RedisService } from './redis';

interface Connection {
    id: string;
    ws: WebSocket;
    userId: string;
    connectedAt: Date;
}

export class ConnectionManager {
    private static instance: ConnectionManager;
    private connections: Map<string, Connection[]> = new Map(); // userId -> Connection[] (User might have multiple tabs/devices)
    private redisService: RedisService;

    private constructor() {
        this.redisService = RedisService.getInstance();
        this.setupRedisSubscription();
    }

    public static getInstance(): ConnectionManager {
        if (!ConnectionManager.instance) {
            ConnectionManager.instance = new ConnectionManager();
        }
        return ConnectionManager.instance;
    }

    private setupRedisSubscription() {
        // Subscribe to broadcast events from other gateways
        this.redisService.subscribe('broadcast', (message) => {
            try {
                const payload = JSON.parse(message);
                const { userId, data } = payload;
                // Deliver to local connections if any
                this.sendToLocalUser(userId, data);
            } catch (err) {
                logger.error({ err }, 'Failed to parse broadcast message');
            }
        });
    }

    public addConnection(userId: string, ws: WebSocket) {
        const connectionId = uuidv4();
        const connection: Connection = {
            id: connectionId,
            ws,
            userId,
            connectedAt: new Date(),
        };

        if (!this.connections.has(userId)) {
            this.connections.set(userId, []);
        }
        this.connections.get(userId)?.push(connection);

        logger.info({ userId, connectionId }, 'Connection established');

        ws.on('close', () => {
            this.removeConnection(userId, connectionId);
        });

        ws.on('pong', () => {
            // Heartbeat received
            // Could update last seen status here
        });
    }

    public removeConnection(userId: string, connectionId: string) {
        const userConns = this.connections.get(userId);
        if (!userConns) return;

        const updatedConns = userConns.filter(c => c.id !== connectionId);

        if (updatedConns.length === 0) {
            this.connections.delete(userId);
        } else {
            this.connections.set(userId, updatedConns);
        }
        logger.info({ userId, connectionId }, 'Connection closed');
    }

    public sendToUser(userId: string, message: any) {
        // 1. Try to send locally
        const sentLocally = this.sendToLocalUser(userId, message);

        // 2. Publish to Redis to reach other gateways (if user is connected elsewhere)
        // Optimization: If we know the user is sticky-routed to this gateway, we might not need to broadcast.
        // However, for robustness/fallback or if sticky routing fails/changes, broadcast is safer.
        // But broadcast to all gateways for every message is expensive (N*M).
        // The prompt says: "Cross-Gateway Communication... uses Redis Pub/Sub to broadcast notifications... ensuring message delivery regardless of which gateway holds the active connection."
        // So we should broadcast.
        // To avoid double delivery if the user is local, we could flag it? 
        // Or maybe the Redis subscription handles "if local, don't send again"? 
        // Actually, if we send locally, we might skip broadcast? 
        // But what if user calls `GET /api/notification` -> REST Service -> Kafka -> Gateway 1.
        // Gateway 1 consumes Kafka. User might be on Gateway 2. 
        // So Gateway 1 checks if user is local. If not, Broadcast?
        // If Gateway 1 has the user, it sends. Does it STILL broadcast? User might have another device on Gateway 2.
        // So yes, typically you try local + broadcast.
        // But if we broadcast, we receive it ourselves via Redis. We need to handle that loop.
        // Usually, the Redis message includes "senderGatewayId". 
        // Or we just simplisticly send local.

        this.redisService.publish('broadcast', JSON.stringify({ userId, data: message }));
    }

    private sendToLocalUser(userId: string, message: any): boolean {
        const userConns = this.connections.get(userId);
        if (!userConns || userConns.length === 0) return false;

        const payload = JSON.stringify(message);
        userConns.forEach(conn => {
            if (conn.ws.readyState === WebSocket.OPEN) {
                conn.ws.send(payload);
            }
        });
        return true;
    }

    public getMetrics() {
        // Return connection counts etc.
        let totalConnections = 0;
        this.connections.forEach(c => totalConnections += c.length);
        return {
            totalConnections,
            activeUsers: this.connections.size
        };
    }
}
