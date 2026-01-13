import { Request, Response, NextFunction } from 'express';
import Redis from 'ioredis';
import { config } from '../config/config';
import { logger } from '../utils/logger';

/**
 * Token Bucket Rate Limiter
 * 
 * Implements the token bucket algorithm for rate limiting:
 * - Each client has a bucket with a maximum capacity
 * - Tokens are consumed on each request
 * - Tokens are refilled at a configurable rate
 * - Requests are rejected when the bucket is empty
 */

interface TokenBucket {
    tokens: number;
    lastRefill: number;
}

export class RateLimiter {
    private static instance: RateLimiter;
    private redis: Redis | null = null;
    private localBuckets: Map<string, TokenBucket> = new Map();
    private bucketSize: number;
    private refillRate: number;
    private refillIntervalMs: number;

    private constructor() {
        this.bucketSize = config.rateLimit.bucketSize;
        this.refillRate = config.rateLimit.refillRate;
        this.refillIntervalMs = config.rateLimit.refillIntervalMs;
    }

    public static getInstance(): RateLimiter {
        if (!RateLimiter.instance) {
            RateLimiter.instance = new RateLimiter();
        }
        return RateLimiter.instance;
    }

    /**
     * Set Redis client for distributed rate limiting
     */
    public setRedisClient(redis: Redis): void {
        this.redis = redis;
    }

    /**
     * Check if a request is allowed and consume a token
     * Returns { allowed: boolean, remaining: number, resetMs: number }
     */
    public async checkLimit(key: string): Promise<{
        allowed: boolean;
        remaining: number;
        resetMs: number;
    }> {
        const now = Date.now();

        if (this.redis) {
            return this.checkLimitRedis(key, now);
        } else {
            return this.checkLimitLocal(key, now);
        }
    }

    /**
     * Local (in-memory) rate limiting - for single instance or fallback
     */
    private checkLimitLocal(key: string, now: number): {
        allowed: boolean;
        remaining: number;
        resetMs: number;
    } {
        let bucket = this.localBuckets.get(key);

        if (!bucket) {
            bucket = {
                tokens: this.bucketSize,
                lastRefill: now,
            };
            this.localBuckets.set(key, bucket);
        }

        // Refill tokens based on elapsed time
        const elapsed = now - bucket.lastRefill;
        const tokensToAdd = Math.floor(elapsed / this.refillIntervalMs) * this.refillRate;

        if (tokensToAdd > 0) {
            bucket.tokens = Math.min(this.bucketSize, bucket.tokens + tokensToAdd);
            bucket.lastRefill = now;
        }

        // Calculate reset time (when bucket will have at least 1 token)
        const resetMs = bucket.tokens < 1
            ? this.refillIntervalMs - (elapsed % this.refillIntervalMs)
            : 0;

        if (bucket.tokens >= 1) {
            bucket.tokens -= 1;
            return {
                allowed: true,
                remaining: Math.floor(bucket.tokens),
                resetMs,
            };
        }

        return {
            allowed: false,
            remaining: 0,
            resetMs,
        };
    }

    /**
     * Redis-based rate limiting - for distributed deployments
     * Uses Lua script for atomic operations
     */
    private async checkLimitRedis(key: string, now: number): Promise<{
        allowed: boolean;
        remaining: number;
        resetMs: number;
    }> {
        const redisKey = `ratelimit:bucket:${key}`;

        const luaScript = `
            local key = KEYS[1]
            local bucket_size = tonumber(ARGV[1])
            local refill_rate = tonumber(ARGV[2])
            local refill_interval_ms = tonumber(ARGV[3])
            local now = tonumber(ARGV[4])
            
            local bucket = redis.call('HGETALL', key)
            local tokens = bucket_size
            local last_refill = now
            
            if #bucket > 0 then
                for i = 1, #bucket, 2 do
                    if bucket[i] == 'tokens' then tokens = tonumber(bucket[i+1]) end
                    if bucket[i] == 'last_refill' then last_refill = tonumber(bucket[i+1]) end
                end
                
                -- Refill tokens based on elapsed time
                local elapsed = now - last_refill
                local tokens_to_add = math.floor(elapsed / refill_interval_ms) * refill_rate
                if tokens_to_add > 0 then
                    tokens = math.min(bucket_size, tokens + tokens_to_add)
                    last_refill = now
                end
            end
            
            local allowed = 0
            local remaining = tokens
            local reset_ms = 0
            
            if tokens >= 1 then
                tokens = tokens - 1
                remaining = tokens
                allowed = 1
            else
                reset_ms = refill_interval_ms - ((now - last_refill) % refill_interval_ms)
            end
            
            redis.call('HSET', key, 'tokens', tokens, 'last_refill', last_refill)
            redis.call('EXPIRE', key, 300) -- 5 minute TTL
            
            return {allowed, math.floor(remaining), reset_ms}
        `;

        try {
            const result = await this.redis!.eval(
                luaScript,
                1,
                redisKey,
                this.bucketSize.toString(),
                this.refillRate.toString(),
                this.refillIntervalMs.toString(),
                now.toString()
            ) as [number, number, number];

            return {
                allowed: result[0] === 1,
                remaining: result[1],
                resetMs: result[2],
            };
        } catch (error) {
            logger.error({ error }, 'Redis rate limit check failed, falling back to local');
            return this.checkLimitLocal(key, now);
        }
    }

    /**
     * Clean up expired local buckets (call periodically)
     */
    public cleanupLocalBuckets(): void {
        const now = Date.now();
        const expirationMs = 5 * 60 * 1000; // 5 minutes

        for (const [key, bucket] of this.localBuckets.entries()) {
            if (now - bucket.lastRefill > expirationMs) {
                this.localBuckets.delete(key);
            }
        }
    }

    /**
     * Get current bucket status for a key (for metrics/debugging)
     */
    public async getBucketStatus(key: string): Promise<TokenBucket | null> {
        if (this.redis) {
            const redisKey = `ratelimit:bucket:${key}`;
            const data = await this.redis.hgetall(redisKey);
            if (data.tokens && data.last_refill) {
                return {
                    tokens: parseFloat(data.tokens),
                    lastRefill: parseInt(data.last_refill, 10),
                };
            }
            return null;
        }
        return this.localBuckets.get(key) || null;
    }
}

/**
 * Express middleware for rate limiting HTTP endpoints
 */
export function rateLimitMiddleware(keyExtractor?: (req: Request) => string) {
    const limiter = RateLimiter.getInstance();

    return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
        const key = keyExtractor
            ? keyExtractor(req)
            : req.ip || req.socket.remoteAddress || 'unknown';

        try {
            const result = await limiter.checkLimit(`http:${key}`);

            // Set rate limit headers
            res.setHeader('X-RateLimit-Limit', config.rateLimit.bucketSize);
            res.setHeader('X-RateLimit-Remaining', result.remaining);
            res.setHeader('X-RateLimit-Reset', Math.ceil(result.resetMs / 1000));

            if (!result.allowed) {
                logger.warn({ key, remaining: result.remaining }, 'Rate limit exceeded');
                res.status(429).json({
                    error: 'Too Many Requests',
                    message: 'Rate limit exceeded. Please try again later.',
                    retryAfterMs: result.resetMs,
                });
                return;
            }

            next();
        } catch (error) {
            logger.error({ error }, 'Rate limit middleware error');
            // On error, allow the request but log it
            next();
        }
    };
}

/**
 * WebSocket upgrade rate limiter
 * Returns true if connection is allowed, false otherwise
 */
export async function checkWebSocketRateLimit(ip: string): Promise<{
    allowed: boolean;
    remaining: number;
    resetMs: number;
}> {
    const limiter = RateLimiter.getInstance();
    return limiter.checkLimit(`ws:${ip}`);
}

// Cleanup interval for local buckets
setInterval(() => {
    RateLimiter.getInstance().cleanupLocalBuckets();
}, 60000); // Every minute
