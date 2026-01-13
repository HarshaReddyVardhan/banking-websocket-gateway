import { Request, Response, NextFunction } from 'express';
import { config } from '../config/config';
import { logger } from '../utils/logger';

/**
 * CORS Middleware
 * 
 * Implements strict Cross-Origin Resource Sharing controls:
 * - Whitelist-based origin validation
 * - Configurable allowed methods and headers
 * - Preflight request handling
 */

export function corsMiddleware(req: Request, res: Response, next: NextFunction): void {
    const origin = req.headers.origin;
    const allowedOrigins = config.cors.allowedOrigins;

    // If no origins configured, only allow same-origin requests
    if (allowedOrigins.length === 0) {
        if (origin) {
            logger.debug({ origin }, 'CORS: Blocking request from origin (no origins configured)');
            res.status(403).json({
                error: 'Forbidden',
                message: 'Cross-origin requests are not allowed',
            });
            return;
        }
        // No origin header = same-origin request, allow it
        next();
        return;
    }

    // Check if origin is in the whitelist
    const isAllowed = origin && (
        allowedOrigins.includes('*') ||
        allowedOrigins.includes(origin) ||
        allowedOrigins.some(pattern => matchOriginPattern(pattern, origin))
    );

    if (origin && !isAllowed) {
        logger.warn({ origin, allowedOrigins }, 'CORS: Blocking request from untrusted origin');
        res.status(403).json({
            error: 'Forbidden',
            message: 'Origin not allowed',
        });
        return;
    }

    // Set CORS headers
    if (origin && isAllowed) {
        // Don't use * when credentials are involved
        res.setHeader('Access-Control-Allow-Origin', origin);
        res.setHeader('Access-Control-Allow-Credentials', 'true');
    }

    res.setHeader('Access-Control-Allow-Methods', config.cors.allowedMethods.join(', '));
    res.setHeader('Access-Control-Allow-Headers', config.cors.allowedHeaders.join(', '));
    res.setHeader('Access-Control-Max-Age', config.cors.maxAge.toString());

    // Handle preflight requests
    if (req.method === 'OPTIONS') {
        res.status(204).end();
        return;
    }

    next();
}

/**
 * Match origin against a pattern (supports wildcards for subdomains)
 * Example: "*.example.com" matches "api.example.com", "www.example.com"
 */
function matchOriginPattern(pattern: string, origin: string): boolean {
    if (!pattern.includes('*')) {
        return pattern === origin;
    }

    // Convert pattern to regex
    const regexPattern = pattern
        .replace(/\./g, '\\.')
        .replace(/\*/g, '[^.]+');

    try {
        const regex = new RegExp(`^${regexPattern}$`);
        return regex.test(origin);
    } catch {
        return false;
    }
}

/**
 * WebSocket origin validator
 * Used during WebSocket upgrade to validate origin header
 */
export function validateWebSocketOrigin(origin: string | undefined): boolean {
    const allowedOrigins = config.cors.allowedOrigins;

    // If no origins configured and there's an origin header, be restrictive in production
    if (allowedOrigins.length === 0) {
        if (config.env === 'production' && origin) {
            logger.warn({ origin }, 'WebSocket: Origin header present but no allowed origins configured');
            return false;
        }
        return true; // Allow in development
    }

    // No origin is okay for same-origin connections
    if (!origin) {
        return true;
    }

    // Check whitelist
    const isAllowed = allowedOrigins.includes('*') ||
        allowedOrigins.includes(origin) ||
        allowedOrigins.some(pattern => matchOriginPattern(pattern, origin));

    if (!isAllowed) {
        logger.warn({ origin, allowedOrigins }, 'WebSocket: Blocking connection from untrusted origin');
    }

    return isAllowed;
}

/**
 * Security headers middleware
 * Adds additional security headers to all responses
 */
export function securityHeadersMiddleware(req: Request, res: Response, next: NextFunction): void {
    // Prevent clickjacking
    res.setHeader('X-Frame-Options', 'DENY');

    // Prevent MIME type sniffing
    res.setHeader('X-Content-Type-Options', 'nosniff');

    // Enable XSS filter
    res.setHeader('X-XSS-Protection', '1; mode=block');

    // Control referrer information
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');

    // Content Security Policy
    res.setHeader('Content-Security-Policy', "default-src 'self'; frame-ancestors 'none';");

    // Strict Transport Security (for HTTPS)
    if (config.env === 'production') {
        res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
    }

    next();
}
