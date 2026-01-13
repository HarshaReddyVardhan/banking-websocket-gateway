import { RateLimiter, checkWebSocketRateLimit } from '../../src/middleware/rate-limiter';

// Reset singleton between tests
beforeEach(() => {
    // Clear internal state by accessing private properties
    const limiter = RateLimiter.getInstance() as any;
    limiter.localBuckets.clear();
});

describe('RateLimiter', () => {
    describe('Token Bucket Algorithm', () => {
        it('should allow requests within bucket size', async () => {
            const limiter = RateLimiter.getInstance();

            // Default bucket size is 10
            for (let i = 0; i < 10; i++) {
                const result = await limiter.checkLimit('test-key');
                expect(result.allowed).toBe(true);
                expect(result.remaining).toBe(9 - i);
            }
        });

        it('should reject requests when bucket is empty', async () => {
            const limiter = RateLimiter.getInstance();

            // Exhaust the bucket
            for (let i = 0; i < 10; i++) {
                await limiter.checkLimit('test-key-exhaust');
            }

            // Next request should be rejected
            const result = await limiter.checkLimit('test-key-exhaust');
            expect(result.allowed).toBe(false);
            expect(result.remaining).toBe(0);
            expect(result.resetMs).toBeGreaterThan(0);
        });

        it('should track different keys independently', async () => {
            const limiter = RateLimiter.getInstance();

            // Use up key1's bucket
            for (let i = 0; i < 10; i++) {
                await limiter.checkLimit('key1');
            }

            // key2 should still have full bucket
            const result = await limiter.checkLimit('key2');
            expect(result.allowed).toBe(true);
            expect(result.remaining).toBe(9);
        });

        it('should refill tokens over time', async () => {
            jest.useFakeTimers();
            const limiter = RateLimiter.getInstance();

            // Exhaust the bucket
            for (let i = 0; i < 10; i++) {
                await limiter.checkLimit('refill-key');
            }

            // Verify bucket is empty
            let result = await limiter.checkLimit('refill-key');
            expect(result.allowed).toBe(false);

            // Advance time by refill interval (default 1000ms)
            jest.advanceTimersByTime(1000);

            // Should have 1 token now
            result = await limiter.checkLimit('refill-key');
            expect(result.allowed).toBe(true);

            jest.useRealTimers();
        });

        it('should not exceed bucket size on refill', async () => {
            jest.useFakeTimers();
            const limiter = RateLimiter.getInstance();

            // Use a few tokens
            await limiter.checkLimit('cap-key');
            await limiter.checkLimit('cap-key');

            // Wait for many refill intervals
            jest.advanceTimersByTime(20000); // 20 seconds

            // Check bucket status
            const status = await limiter.getBucketStatus('cap-key');
            expect(status).not.toBeNull();
            expect(status!.tokens).toBeLessThanOrEqual(10); // Bucket size

            jest.useRealTimers();
        });
    });

    describe('Bucket Cleanup', () => {
        it('should clean up expired buckets', async () => {
            jest.useFakeTimers();
            const limiter = RateLimiter.getInstance();

            // Create a bucket
            await limiter.checkLimit('cleanup-key');

            // Verify it exists
            let status = await limiter.getBucketStatus('cleanup-key');
            expect(status).not.toBeNull();

            // Advance time past expiration (5 minutes)
            jest.advanceTimersByTime(6 * 60 * 1000);

            // Run cleanup
            limiter.cleanupLocalBuckets();

            // Bucket should be gone
            status = await limiter.getBucketStatus('cleanup-key');
            expect(status).toBeNull();

            jest.useRealTimers();
        });
    });
});

describe('checkWebSocketRateLimit', () => {
    beforeEach(() => {
        const limiter = RateLimiter.getInstance() as any;
        limiter.localBuckets.clear();
    });

    it('should return allowed for first connections', async () => {
        const result = await checkWebSocketRateLimit('192.168.1.1');
        expect(result.allowed).toBe(true);
    });

    it('should prefix keys with ws:', async () => {
        // Call the function
        await checkWebSocketRateLimit('192.168.1.2');

        // Check that the bucket was created with ws: prefix
        const limiter = RateLimiter.getInstance();
        const status = await limiter.getBucketStatus('ws:192.168.1.2');
        expect(status).not.toBeNull();
    });

    it('should enforce rate limits per IP', async () => {
        // Exhaust the bucket for an IP
        for (let i = 0; i < 10; i++) {
            await checkWebSocketRateLimit('192.168.1.3');
        }

        // Next connection should be rejected
        const result = await checkWebSocketRateLimit('192.168.1.3');
        expect(result.allowed).toBe(false);
    });
});
