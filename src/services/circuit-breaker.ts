import { logger } from '../utils/logger';
import { config } from '../config/config';

/**
 * Circuit Breaker Pattern Implementation
 * 
 * Prevents cascading failures by temporarily stopping calls to failing services.
 * States:
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: Service failing, requests immediately rejected
 * - HALF_OPEN: Testing if service recovered, limited requests allowed
 */

export enum CircuitState {
    CLOSED = 'CLOSED',
    OPEN = 'OPEN',
    HALF_OPEN = 'HALF_OPEN',
}

interface CircuitBreakerOptions {
    name: string;
    failureThreshold?: number;
    recoveryTimeoutMs?: number;
    halfOpenMaxCalls?: number;
}

export class CircuitBreaker {
    private state: CircuitState = CircuitState.CLOSED;
    private failureCount: number = 0;
    private successCount: number = 0;
    private lastFailureTime: number = 0;
    private halfOpenCalls: number = 0;

    private readonly name: string;
    private readonly failureThreshold: number;
    private readonly recoveryTimeoutMs: number;
    private readonly halfOpenMaxCalls: number;

    constructor(options: CircuitBreakerOptions) {
        this.name = options.name;
        this.failureThreshold = options.failureThreshold ?? config.circuitBreaker.failureThreshold;
        this.recoveryTimeoutMs = options.recoveryTimeoutMs ?? config.circuitBreaker.recoveryTimeoutMs;
        this.halfOpenMaxCalls = options.halfOpenMaxCalls ?? config.circuitBreaker.halfOpenMaxCalls;
    }

    /**
     * Execute a function with circuit breaker protection
     */
    public async execute<T>(fn: () => Promise<T>): Promise<T> {
        // Check if circuit should transition from OPEN to HALF_OPEN
        if (this.state === CircuitState.OPEN) {
            if (Date.now() - this.lastFailureTime >= this.recoveryTimeoutMs) {
                this.transitionTo(CircuitState.HALF_OPEN);
            } else {
                throw new CircuitOpenError(
                    this.name,
                    this.recoveryTimeoutMs - (Date.now() - this.lastFailureTime)
                );
            }
        }

        // In HALF_OPEN, limit the number of test calls
        if (this.state === CircuitState.HALF_OPEN) {
            if (this.halfOpenCalls >= this.halfOpenMaxCalls) {
                throw new CircuitOpenError(this.name, 0);
            }
            this.halfOpenCalls++;
        }

        try {
            const result = await fn();
            this.onSuccess();
            return result;
        } catch (error) {
            this.onFailure();
            throw error;
        }
    }

    /**
     * Record a successful call
     */
    private onSuccess(): void {
        this.failureCount = 0;
        this.successCount++;

        if (this.state === CircuitState.HALF_OPEN) {
            // Successful call in HALF_OPEN, close the circuit
            this.transitionTo(CircuitState.CLOSED);
        }
    }

    /**
     * Record a failed call
     */
    private onFailure(): void {
        this.failureCount++;
        this.lastFailureTime = Date.now();

        if (this.state === CircuitState.HALF_OPEN) {
            // Failed in HALF_OPEN, reopen the circuit
            this.transitionTo(CircuitState.OPEN);
        } else if (this.state === CircuitState.CLOSED && this.failureCount >= this.failureThreshold) {
            // Too many failures in CLOSED, open the circuit
            this.transitionTo(CircuitState.OPEN);
        }
    }

    /**
     * Transition to a new state
     */
    private transitionTo(newState: CircuitState): void {
        const previousState = this.state;
        this.state = newState;

        if (newState === CircuitState.HALF_OPEN) {
            this.halfOpenCalls = 0;
        }

        if (newState === CircuitState.CLOSED) {
            this.failureCount = 0;
            this.successCount = 0;
        }

        logger.info({
            circuitBreaker: this.name,
            previousState,
            newState,
            failureCount: this.failureCount,
        }, `Circuit breaker ${this.name} state changed: ${previousState} -> ${newState}`);
    }

    /**
     * Get current circuit state and metrics
     */
    public getStatus(): {
        name: string;
        state: CircuitState;
        failureCount: number;
        successCount: number;
        lastFailureTime: number | null;
    } {
        return {
            name: this.name,
            state: this.state,
            failureCount: this.failureCount,
            successCount: this.successCount,
            lastFailureTime: this.lastFailureTime || null,
        };
    }

    /**
     * Force the circuit to a specific state (for testing/admin)
     */
    public forceState(state: CircuitState): void {
        logger.warn({ circuitBreaker: this.name, state }, 'Circuit breaker state forced');
        this.transitionTo(state);
    }

    /**
     * Reset the circuit breaker
     */
    public reset(): void {
        this.state = CircuitState.CLOSED;
        this.failureCount = 0;
        this.successCount = 0;
        this.lastFailureTime = 0;
        this.halfOpenCalls = 0;
        logger.info({ circuitBreaker: this.name }, 'Circuit breaker reset');
    }
}

/**
 * Error thrown when circuit is open
 */
export class CircuitOpenError extends Error {
    public readonly circuitName: string;
    public readonly retryAfterMs: number;

    constructor(circuitName: string, retryAfterMs: number) {
        super(`Circuit breaker '${circuitName}' is open. Retry after ${retryAfterMs}ms`);
        this.name = 'CircuitOpenError';
        this.circuitName = circuitName;
        this.retryAfterMs = retryAfterMs;
    }
}

/**
 * Circuit breaker registry for managing multiple circuits
 */
class CircuitBreakerRegistry {
    private static instance: CircuitBreakerRegistry;
    private circuits: Map<string, CircuitBreaker> = new Map();

    private constructor() { }

    public static getInstance(): CircuitBreakerRegistry {
        if (!CircuitBreakerRegistry.instance) {
            CircuitBreakerRegistry.instance = new CircuitBreakerRegistry();
        }
        return CircuitBreakerRegistry.instance;
    }

    /**
     * Get or create a circuit breaker
     */
    public getCircuit(options: CircuitBreakerOptions): CircuitBreaker {
        let circuit = this.circuits.get(options.name);
        if (!circuit) {
            circuit = new CircuitBreaker(options);
            this.circuits.set(options.name, circuit);
        }
        return circuit;
    }

    /**
     * Get all circuit statuses
     */
    public getAllStatuses(): ReturnType<CircuitBreaker['getStatus']>[] {
        return Array.from(this.circuits.values()).map(c => c.getStatus());
    }

    /**
     * Reset all circuits
     */
    public resetAll(): void {
        this.circuits.forEach(c => c.reset());
    }
}

export const circuitBreakerRegistry = CircuitBreakerRegistry.getInstance();

// Create pre-configured circuit breakers for common services
export const redisCircuitBreaker = circuitBreakerRegistry.getCircuit({ name: 'redis' });
export const kafkaCircuitBreaker = circuitBreakerRegistry.getCircuit({ name: 'kafka' });
