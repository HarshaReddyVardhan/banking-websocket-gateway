import { CircuitBreaker, CircuitState, CircuitOpenError, circuitBreakerRegistry } from '../../src/services/circuit-breaker';

describe('CircuitBreaker', () => {
    let breaker: CircuitBreaker;

    beforeEach(() => {
        breaker = new CircuitBreaker({
            name: 'test-breaker',
            failureThreshold: 3,
            recoveryTimeoutMs: 1000,
            halfOpenMaxCalls: 2,
        });
    });

    describe('CLOSED State', () => {
        it('should start in CLOSED state', () => {
            expect(breaker.getStatus().state).toBe(CircuitState.CLOSED);
        });

        it('should allow successful calls', async () => {
            const result = await breaker.execute(async () => 'success');
            expect(result).toBe('success');
        });

        it('should count failures and stay CLOSED below threshold', async () => {
            // Fail twice (threshold is 3)
            for (let i = 0; i < 2; i++) {
                await expect(
                    breaker.execute(async () => { throw new Error('fail'); })
                ).rejects.toThrow('fail');
            }

            expect(breaker.getStatus().state).toBe(CircuitState.CLOSED);
            expect(breaker.getStatus().failureCount).toBe(2);
        });

        it('should transition to OPEN after failure threshold', async () => {
            // Fail 3 times (threshold)
            for (let i = 0; i < 3; i++) {
                await expect(
                    breaker.execute(async () => { throw new Error('fail'); })
                ).rejects.toThrow('fail');
            }

            expect(breaker.getStatus().state).toBe(CircuitState.OPEN);
        });

        it('should reset failure count on success', async () => {
            // Fail twice
            for (let i = 0; i < 2; i++) {
                await expect(
                    breaker.execute(async () => { throw new Error('fail'); })
                ).rejects.toThrow('fail');
            }

            // Now succeed
            await breaker.execute(async () => 'success');

            expect(breaker.getStatus().failureCount).toBe(0);
        });
    });

    describe('OPEN State', () => {
        beforeEach(async () => {
            // Open the circuit
            for (let i = 0; i < 3; i++) {
                try {
                    await breaker.execute(async () => { throw new Error('fail'); });
                } catch { }
            }
        });

        it('should reject calls immediately with CircuitOpenError', async () => {
            await expect(
                breaker.execute(async () => 'success')
            ).rejects.toThrow(CircuitOpenError);
        });

        it('should include retry time in error', async () => {
            try {
                await breaker.execute(async () => 'success');
                fail('Should have thrown');
            } catch (error) {
                expect(error).toBeInstanceOf(CircuitOpenError);
                expect((error as CircuitOpenError).retryAfterMs).toBeLessThanOrEqual(1000);
            }
        });

        it('should transition to HALF_OPEN after recovery timeout', async () => {
            jest.useFakeTimers();

            // Advance time past recovery timeout
            jest.advanceTimersByTime(1100);

            // Next call should be allowed (circuit is HALF_OPEN)
            try {
                await breaker.execute(async () => 'success');
            } catch { }

            expect(breaker.getStatus().state).toBe(CircuitState.HALF_OPEN);

            jest.useRealTimers();
        });
    });

    describe('HALF_OPEN State', () => {
        beforeEach(async () => {
            jest.useFakeTimers();

            // Open the circuit
            for (let i = 0; i < 3; i++) {
                try {
                    await breaker.execute(async () => { throw new Error('fail'); });
                } catch { }
            }

            // Wait for recovery timeout
            jest.advanceTimersByTime(1100);
        });

        afterEach(() => {
            jest.useRealTimers();
        });

        it('should allow limited test calls', async () => {
            // First call should be allowed
            await breaker.execute(async () => 'success');
            expect(breaker.getStatus().state).toBe(CircuitState.CLOSED);
        });

        it('should transition to CLOSED on success', async () => {
            await breaker.execute(async () => 'success');
            expect(breaker.getStatus().state).toBe(CircuitState.CLOSED);
        });

        it('should transition back to OPEN on failure', async () => {
            await expect(
                breaker.execute(async () => { throw new Error('fail'); })
            ).rejects.toThrow('fail');

            expect(breaker.getStatus().state).toBe(CircuitState.OPEN);
        });

        it('should limit calls in HALF_OPEN', async () => {
            // Use up the limit
            for (let i = 0; i < 2; i++) {
                try {
                    await breaker.execute(async () => { throw new Error('fail'); });
                } catch { }
            }

            // Next call should be rejected
            await expect(
                breaker.execute(async () => 'success')
            ).rejects.toThrow(CircuitOpenError);
        });
    });

    describe('Reset and Force', () => {
        it('should reset to initial state', async () => {
            // Open the circuit
            for (let i = 0; i < 3; i++) {
                try {
                    await breaker.execute(async () => { throw new Error('fail'); });
                } catch { }
            }

            expect(breaker.getStatus().state).toBe(CircuitState.OPEN);

            // Reset
            breaker.reset();

            expect(breaker.getStatus().state).toBe(CircuitState.CLOSED);
            expect(breaker.getStatus().failureCount).toBe(0);
        });

        it('should allow forcing state', () => {
            breaker.forceState(CircuitState.OPEN);
            expect(breaker.getStatus().state).toBe(CircuitState.OPEN);

            breaker.forceState(CircuitState.HALF_OPEN);
            expect(breaker.getStatus().state).toBe(CircuitState.HALF_OPEN);
        });
    });
});

describe('CircuitBreakerRegistry', () => {
    it('should return same circuit for same name', () => {
        const circuit1 = circuitBreakerRegistry.getCircuit({ name: 'test-registry' });
        const circuit2 = circuitBreakerRegistry.getCircuit({ name: 'test-registry' });
        expect(circuit1).toBe(circuit2);
    });

    it('should return different circuits for different names', () => {
        const circuit1 = circuitBreakerRegistry.getCircuit({ name: 'test-1' });
        const circuit2 = circuitBreakerRegistry.getCircuit({ name: 'test-2' });
        expect(circuit1).not.toBe(circuit2);
    });

    it('should get all statuses', () => {
        circuitBreakerRegistry.getCircuit({ name: 'status-test-1' });
        circuitBreakerRegistry.getCircuit({ name: 'status-test-2' });

        const statuses = circuitBreakerRegistry.getAllStatuses();
        expect(statuses.length).toBeGreaterThanOrEqual(2);
        expect(statuses.some(s => s.name === 'status-test-1')).toBe(true);
        expect(statuses.some(s => s.name === 'status-test-2')).toBe(true);
    });
});
