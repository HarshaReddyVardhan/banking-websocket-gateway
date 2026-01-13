import { WebSocketServer, WebSocket } from 'ws';
import { IncomingMessage } from 'http';
import { URL } from 'url';
import jwt from 'jsonwebtoken';
import { config } from '../config/config';
import { logger } from '../utils/logger';
import { ConnectionManager } from './connection-manager';
import { RedisService } from './redis';
import { checkWebSocketRateLimit, RateLimiter } from '../middleware/rate-limiter';
import { validateWebSocketOrigin } from '../middleware/cors';
import { auditLogger, AuditEventType } from '../middleware/audit-logger';
import { safeWebSocketHandler } from '../middleware/panic-recovery';
import { z } from 'zod';

/**
 * WebSocket Message Schema for validation
 */
const WebSocketMessageSchema = z.object({
    type: z.string().min(1).max(50),
    payload: z.unknown().optional(),
    requestId: z.string().uuid().optional(),
});

export class WebSocketGateway {
    private wss: WebSocketServer;
    private connectionManager: ConnectionManager;
    private redisService: RedisService;

    constructor(server: any) {
        this.wss = new WebSocketServer({ noServer: true });
        this.connectionManager = ConnectionManager.getInstance();
        this.redisService = RedisService.getInstance();

        // Initialize rate limiter with Redis
        RateLimiter.getInstance().setRedisClient(this.redisService.getPubClient());

        server.on('upgrade', this.handleUpgrade.bind(this));

        this.wss.on('connection', safeWebSocketHandler((ws: WebSocket, request: IncomingMessage, userId: string) => {
            const ip = this.getClientIp(request);
            const connectionId = this.connectionManager.addConnection(userId, ws, ip);

            // Log connection established
            auditLogger.logConnectionEstablished({ ip, userId, connectionId });

            ws.on('message', safeWebSocketHandler((message: Buffer) => {
                this.handleMessage(ws, userId, message);
            }, 'ws.onMessage'));

            ws.on('error', (error: Error) => {
                logger.error({ error, userId, connectionId }, 'WebSocket error');
            });
        }, 'wss.onConnection'));

        logger.info('WebSocket Gateway initialized');
    }

    private async handleUpgrade(request: IncomingMessage, socket: any, head: Buffer) {
        const { url } = request;
        const ip = this.getClientIp(request);
        const userAgent = request.headers['user-agent'];

        if (!url) {
            auditLogger.logConnectionAttempt({
                ip,
                userAgent,
                success: false,
                reason: 'Missing URL',
            });
            socket.destroy();
            return;
        }

        try {
            const parsedUrl = new URL(url, `http://${request.headers.host}`);
            const token = parsedUrl.searchParams.get('token');

            // 1. Origin Validation
            const origin = request.headers.origin;
            if (!validateWebSocketOrigin(origin)) {
                auditLogger.logOriginBlocked({ ip, origin: origin || 'none' });
                this.reject(socket, 403, 'Forbidden');
                return;
            }

            // 2. Token Required
            if (!token) {
                auditLogger.logAuthFailure({ ip, reason: 'Missing token' });
                this.reject(socket, 401, 'Unauthorized');
                return;
            }

            // 3. Rate Limiting Check
            const rateLimit = await checkWebSocketRateLimit(ip);
            if (!rateLimit.allowed) {
                auditLogger.logRateLimitExceeded({ ip, limitType: 'connection' });
                this.reject(socket, 429, 'Too Many Requests');
                return;
            }

            // 4. Validate Token
            const payload = await this.validateToken(token, ip);
            if (!payload) {
                this.reject(socket, 401, 'Invalid Token');
                return;
            }

            const userId = (payload as any).sub || (payload as any).userId;
            if (!userId) {
                auditLogger.logAuthFailure({ ip, reason: 'Token missing user identifier' });
                this.reject(socket, 401, 'Invalid Token');
                return;
            }

            // 5. Check max connections per user
            if (!this.connectionManager.canAddConnection(userId)) {
                auditLogger.logConnectionAttempt({
                    ip,
                    userId,
                    userAgent,
                    success: false,
                    reason: 'Max connections exceeded',
                });
                this.reject(socket, 429, 'Too Many Connections');
                return;
            }

            // 6. Log successful connection attempt
            auditLogger.logConnectionAttempt({
                ip,
                userId,
                userAgent,
                success: true,
            });

            // 7. Complete Upgrade
            this.wss.handleUpgrade(request, socket, head, (ws) => {
                this.wss.emit('connection', ws, request, userId);
            });

        } catch (err) {
            logger.error({ err, ip }, 'Connection upgrade failed');
            auditLogger.logConnectionAttempt({
                ip,
                userAgent,
                success: false,
                reason: 'Internal error',
            });
            socket.destroy();
        }
    }

    /**
     * Validate JWT token with comprehensive checks
     */
    private async validateToken(token: string, ip: string): Promise<string | jwt.JwtPayload | null> {
        try {
            // Verify token signature and expiration
            const payload = jwt.verify(token, config.auth.jwtSecret, {
                issuer: config.auth.jwtIssuer,
                audience: config.auth.jwtAudience,
                algorithms: config.auth.jwtAlgorithms,
            });

            // Check for required claims
            if (typeof payload === 'string') {
                auditLogger.logAuthFailure({ ip, reason: 'Invalid token format' });
                return null;
            }

            // Check token revocation (if jti claim exists)
            const jti = payload.jti;
            if (jti) {
                const isRevoked = await this.isTokenRevoked(jti);
                if (isRevoked) {
                    auditLogger.logAuthFailure({
                        ip,
                        reason: 'Token revoked',
                        tokenPreview: token.substring(0, 20) + '...',
                    });
                    return null;
                }
            }

            return payload;
        } catch (err) {
            const errorMessage = err instanceof Error ? err.message : 'Unknown error';
            auditLogger.logAuthFailure({
                ip,
                reason: `Token verification failed: ${errorMessage}`,
                tokenPreview: token.substring(0, 20) + '...',
            });
            return null;
        }
    }

    /**
     * Check if token has been revoked (stored in Redis)
     */
    private async isTokenRevoked(jti: string): Promise<boolean> {
        try {
            const revoked = await this.redisService.safeGet(`token:revoked:${jti}`);
            return revoked !== null;
        } catch (error) {
            logger.error({ error, jti }, 'Error checking token revocation');
            // On error, allow token (fail open) - could change to fail closed for higher security
            return false;
        }
    }

    /**
     * Handle incoming WebSocket messages
     */
    private handleMessage(ws: WebSocket, userId: string, message: Buffer): void {
        try {
            const messageStr = message.toString();

            // Limit message size (prevent DoS)
            if (messageStr.length > 65536) { // 64KB
                logger.warn({ userId, size: messageStr.length }, 'Message too large');
                ws.send(JSON.stringify({
                    type: 'error',
                    error: 'Message too large',
                }));
                return;
            }

            // Parse and validate JSON
            let parsed: unknown;
            try {
                parsed = JSON.parse(messageStr);
            } catch {
                ws.send(JSON.stringify({
                    type: 'error',
                    error: 'Invalid JSON',
                }));
                return;
            }

            // Validate message schema
            const result = WebSocketMessageSchema.safeParse(parsed);
            if (!result.success) {
                ws.send(JSON.stringify({
                    type: 'error',
                    error: 'Invalid message format',
                    details: result.error.issues,
                }));
                return;
            }

            const validatedMessage = result.data;

            // Handle different message types
            switch (validatedMessage.type) {
                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong', requestId: validatedMessage.requestId }));
                    break;
                case 'subscribe':
                    // Handle subscription requests
                    this.handleSubscription(ws, userId, validatedMessage);
                    break;
                default:
                    // Log unknown message types
                    logger.debug({ userId, type: validatedMessage.type }, 'Unknown message type received');
            }
        } catch (error) {
            logger.error({ error, userId }, 'Error handling WebSocket message');
        }
    }

    /**
     * Handle subscription requests
     */
    private handleSubscription(ws: WebSocket, userId: string, message: z.infer<typeof WebSocketMessageSchema>): void {
        // Implementation for subscription handling
        ws.send(JSON.stringify({
            type: 'subscribed',
            requestId: message.requestId,
        }));
    }

    /**
     * Reject a WebSocket connection with HTTP response
     */
    private reject(socket: any, code: number, message: string): void {
        const headers = [
            `HTTP/1.1 ${code} ${message}`,
            'Content-Type: text/plain',
            'Connection: close',
            '',
            message,
        ].join('\r\n');

        socket.write(headers);
        socket.destroy();
    }

    /**
     * Extract client IP address from request
     */
    private getClientIp(request: IncomingMessage): string {
        // Check for forwarded headers (when behind proxy)
        const forwarded = request.headers['x-forwarded-for'];
        if (forwarded) {
            const ips = Array.isArray(forwarded) ? forwarded[0] : forwarded;
            return ips.split(',')[0].trim();
        }

        const realIp = request.headers['x-real-ip'];
        if (realIp) {
            return Array.isArray(realIp) ? realIp[0] : realIp;
        }

        return request.socket.remoteAddress || 'unknown';
    }

    /**
     * Get WebSocket server for metrics
     */
    public getServer(): WebSocketServer {
        return this.wss;
    }
}
