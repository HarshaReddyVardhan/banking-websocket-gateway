import { Request, Response, NextFunction } from 'express';
import { logger } from '../utils/logger';

/**
 * Panic Recovery Middleware
 * 
 * Provides global error handling to prevent crashes:
 * - Catches unhandled exceptions in async handlers
 * - Handles uncaught exceptions at process level
 * - Handles unhandled promise rejections
 * - Returns safe error responses to clients
 */

/**
 * Async handler wrapper to catch errors in async route handlers
 */
export function asyncHandler<T>(
    fn: (req: Request, res: Response, next: NextFunction) => Promise<T>
) {
    return (req: Request, res: Response, next: NextFunction): void => {
        Promise.resolve(fn(req, res, next)).catch(next);
    };
}

/**
 * Global error handling middleware for Express
 * Place at the end of middleware chain
 */
export function errorHandler(
    err: Error,
    req: Request,
    res: Response,
    _next: NextFunction
): void {
    // Log the full error
    logger.error({
        error: {
            name: err.name,
            message: err.message,
            stack: err.stack,
        },
        request: {
            method: req.method,
            path: req.path,
            ip: req.ip || req.socket.remoteAddress,
        },
    }, 'Unhandled error in request handler');

    // Don't expose error details in production
    const isProduction = process.env.NODE_ENV === 'production';

    res.status(500).json({
        error: 'Internal Server Error',
        message: isProduction ? 'An unexpected error occurred' : err.message,
        ...(isProduction ? {} : { stack: err.stack }),
    });
}

/**
 * Setup global process-level error handlers
 * Call this early in application startup
 */
export function setupGlobalErrorHandlers(): void {
    // Handle uncaught exceptions
    process.on('uncaughtException', (error: Error) => {
        logger.fatal({
            error: {
                name: error.name,
                message: error.message,
                stack: error.stack,
            },
        }, 'FATAL: Uncaught exception - process will exit');

        // Give time for logs to flush
        setTimeout(() => {
            process.exit(1);
        }, 1000);
    });

    // Handle unhandled promise rejections
    process.on('unhandledRejection', (reason: unknown, promise: Promise<unknown>) => {
        logger.error({
            reason: reason instanceof Error ? {
                name: reason.name,
                message: reason.message,
                stack: reason.stack,
            } : reason,
        }, 'Unhandled promise rejection');

        // In Node.js 15+, unhandled rejections crash by default
        // We log but don't crash, allowing graceful handling
    });

    // Handle warnings
    process.on('warning', (warning: Error) => {
        logger.warn({
            warning: {
                name: warning.name,
                message: warning.message,
                stack: warning.stack,
            },
        }, 'Process warning');
    });

    logger.info('Global error handlers initialized');
}

/**
 * Safe wrapper for WebSocket handlers
 * Catches errors and prevents connection crashes
 */
export function safeWebSocketHandler<T extends (...args: any[]) => any>(
    handler: T,
    context: string
): T {
    return ((...args: Parameters<T>) => {
        try {
            const result = handler(...args);
            if (result instanceof Promise) {
                return result.catch((error: Error) => {
                    logger.error({
                        error: {
                            name: error.name,
                            message: error.message,
                            stack: error.stack,
                        },
                        context,
                    }, `Error in WebSocket handler: ${context}`);
                });
            }
            return result;
        } catch (error) {
            logger.error({
                error: error instanceof Error ? {
                    name: error.name,
                    message: error.message,
                    stack: error.stack,
                } : error,
                context,
            }, `Sync error in WebSocket handler: ${context}`);
        }
    }) as T;
}

/**
 * Wrapper for Kafka message handlers
 * Prevents consumer crashes from individual message processing errors
 */
export async function safeKafkaHandler<T>(
    handler: () => Promise<T>,
    context: string
): Promise<T | null> {
    try {
        return await handler();
    } catch (error) {
        logger.error({
            error: error instanceof Error ? {
                name: error.name,
                message: error.message,
                stack: error.stack,
            } : error,
            context,
        }, `Error in Kafka handler: ${context}`);
        return null;
    }
}

/**
 * Timeout wrapper for async operations
 */
export async function withTimeout<T>(
    promise: Promise<T>,
    timeoutMs: number,
    operation: string
): Promise<T> {
    let timeoutId: NodeJS.Timeout;

    const timeoutPromise = new Promise<never>((_, reject) => {
        timeoutId = setTimeout(() => {
            reject(new Error(`Operation timed out after ${timeoutMs}ms: ${operation}`));
        }, timeoutMs);
    });

    try {
        return await Promise.race([promise, timeoutPromise]);
    } finally {
        clearTimeout(timeoutId!);
    }
}
