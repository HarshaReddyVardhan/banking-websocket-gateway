import { Request, Response, NextFunction } from 'express';
import { logger } from '../utils/logger';
import { config } from '../config/config';

/**
 * Audit Logger
 * 
 * Provides comprehensive audit logging for security and compliance:
 * - Connection attempts (success/failure)
 * - Authentication events
 * - Rate limit violations
 * - WebSocket message events
 */

export interface AuditEvent {
    timestamp: string;
    eventType: AuditEventType;
    ip: string;
    userId?: string;
    sessionId?: string;
    userAgent?: string;
    success: boolean;
    reason?: string;
    metadata?: Record<string, unknown>;
}

export enum AuditEventType {
    CONNECTION_ATTEMPT = 'CONNECTION_ATTEMPT',
    CONNECTION_ESTABLISHED = 'CONNECTION_ESTABLISHED',
    CONNECTION_CLOSED = 'CONNECTION_CLOSED',
    AUTHENTICATION_SUCCESS = 'AUTHENTICATION_SUCCESS',
    AUTHENTICATION_FAILURE = 'AUTHENTICATION_FAILURE',
    RATE_LIMIT_EXCEEDED = 'RATE_LIMIT_EXCEEDED',
    MESSAGE_RECEIVED = 'MESSAGE_RECEIVED',
    MESSAGE_SENT = 'MESSAGE_SENT',
    TOKEN_REVOKED = 'TOKEN_REVOKED',
    ORIGIN_BLOCKED = 'ORIGIN_BLOCKED',
    SUSPICIOUS_ACTIVITY = 'SUSPICIOUS_ACTIVITY',
}

class AuditLogger {
    private static instance: AuditLogger;

    private constructor() { }

    public static getInstance(): AuditLogger {
        if (!AuditLogger.instance) {
            AuditLogger.instance = new AuditLogger();
        }
        return AuditLogger.instance;
    }

    /**
     * Log an audit event
     */
    public log(event: Omit<AuditEvent, 'timestamp'>): void {
        const auditEvent: AuditEvent = {
            ...event,
            timestamp: new Date().toISOString(),
        };

        // Sanitize sensitive data
        const sanitizedEvent = this.sanitizeEvent(auditEvent);

        // Log as structured JSON for log aggregation
        logger.info(
            { audit: true, ...sanitizedEvent },
            `AUDIT: ${event.eventType}`
        );
    }

    /**
     * Log a connection attempt
     */
    public logConnectionAttempt(params: {
        ip: string;
        userId?: string;
        userAgent?: string;
        success: boolean;
        reason?: string;
    }): void {
        this.log({
            eventType: AuditEventType.CONNECTION_ATTEMPT,
            ip: params.ip,
            userId: params.userId,
            userAgent: params.userAgent,
            success: params.success,
            reason: params.reason,
        });
    }

    /**
     * Log a successful connection establishment
     */
    public logConnectionEstablished(params: {
        ip: string;
        userId: string;
        connectionId: string;
    }): void {
        this.log({
            eventType: AuditEventType.CONNECTION_ESTABLISHED,
            ip: params.ip,
            userId: params.userId,
            sessionId: params.connectionId,
            success: true,
        });
    }

    /**
     * Log a connection close
     */
    public logConnectionClosed(params: {
        ip: string;
        userId: string;
        connectionId: string;
        reason?: string;
        durationMs?: number;
    }): void {
        this.log({
            eventType: AuditEventType.CONNECTION_CLOSED,
            ip: params.ip,
            userId: params.userId,
            sessionId: params.connectionId,
            success: true,
            reason: params.reason,
            metadata: params.durationMs ? { durationMs: params.durationMs } : undefined,
        });
    }

    /**
     * Log authentication failure
     */
    public logAuthFailure(params: {
        ip: string;
        reason: string;
        tokenPreview?: string;
    }): void {
        this.log({
            eventType: AuditEventType.AUTHENTICATION_FAILURE,
            ip: params.ip,
            success: false,
            reason: params.reason,
            metadata: params.tokenPreview ? { tokenPreview: params.tokenPreview } : undefined,
        });
    }

    /**
     * Log rate limit exceeded
     */
    public logRateLimitExceeded(params: {
        ip: string;
        userId?: string;
        limitType: 'connection' | 'message' | 'http';
    }): void {
        this.log({
            eventType: AuditEventType.RATE_LIMIT_EXCEEDED,
            ip: params.ip,
            userId: params.userId,
            success: false,
            reason: `Rate limit exceeded for ${params.limitType}`,
        });
    }

    /**
     * Log suspicious activity detection
     */
    public logSuspiciousActivity(params: {
        ip: string;
        userId?: string;
        activity: string;
        details?: Record<string, unknown>;
    }): void {
        this.log({
            eventType: AuditEventType.SUSPICIOUS_ACTIVITY,
            ip: params.ip,
            userId: params.userId,
            success: false,
            reason: params.activity,
            metadata: params.details,
        });
    }

    /**
     * Log origin blocked  
     */
    public logOriginBlocked(params: {
        ip: string;
        origin: string;
    }): void {
        this.log({
            eventType: AuditEventType.ORIGIN_BLOCKED,
            ip: params.ip,
            success: false,
            reason: `Blocked origin: ${params.origin}`,
        });
    }

    /**
     * Sanitize sensitive data from event before logging
     */
    private sanitizeEvent(event: AuditEvent): AuditEvent {
        const sanitized = { ...event };

        // Mask partial IP for privacy (keep first 2 octets)
        if (sanitized.ip && config.env === 'production') {
            const parts = sanitized.ip.split('.');
            if (parts.length === 4) {
                sanitized.ip = `${parts[0]}.${parts[1]}.xxx.xxx`;
            }
        }

        // Truncate user agent
        if (sanitized.userAgent && sanitized.userAgent.length > 100) {
            sanitized.userAgent = sanitized.userAgent.substring(0, 100) + '...';
        }

        return sanitized;
    }
}

export const auditLogger = AuditLogger.getInstance();

/**
 * Express middleware for HTTP request audit logging
 */
export function httpAuditMiddleware(req: Request, res: Response, next: NextFunction): void {
    const startTime = Date.now();
    const ip = req.ip || req.socket.remoteAddress || 'unknown';

    res.on('finish', () => {
        const duration = Date.now() - startTime;

        // Only log non-health-check requests
        if (!req.path.includes('/health') && !req.path.includes('/metrics')) {
            logger.info({
                audit: true,
                eventType: 'HTTP_REQUEST',
                ip,
                method: req.method,
                path: req.path,
                statusCode: res.statusCode,
                durationMs: duration,
                userAgent: req.headers['user-agent'],
            }, `HTTP ${req.method} ${req.path} ${res.statusCode}`);
        }
    });

    next();
}
