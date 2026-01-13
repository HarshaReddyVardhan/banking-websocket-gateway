/**
 * WebSocket Gateway Load Test Script
 * 
 * This k6 script simulates high concurrency WebSocket connections
 * to test the gateway's resilience and performance.
 * 
 * Requirements:
 * - k6 installed: https://k6.io/docs/getting-started/installation/
 * - Gateway running at WS_URL (default: ws://localhost:8080)
 * - Valid JWT tokens or a way to generate them
 * 
 * Usage:
 * k6 run tests/load/websocket-load.js
 * 
 * With custom options:
 * k6 run --vus 100 --duration 5m tests/load/websocket-load.js
 */

import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { Counter, Trend, Rate } from 'k6/metrics';
import http from 'k6/http';

// Configuration
const WS_URL = __ENV.WS_URL || 'ws://localhost:8080';
const HTTP_URL = __ENV.HTTP_URL || 'http://localhost:8080';
const JWT_SECRET = __ENV.JWT_SECRET || 'secret';

// Custom Metrics
const wsConnections = new Counter('ws_connections');
const wsConnectionsFailed = new Counter('ws_connections_failed');
const wsMessagesReceived = new Counter('ws_messages_received');
const wsMessagesSent = new Counter('ws_messages_sent');
const wsConnectionDuration = new Trend('ws_connection_duration');
const wsMessageLatency = new Trend('ws_message_latency');
const wsConnectionRate = new Rate('ws_connection_success_rate');

// Test Options
export const options = {
    scenarios: {
        // Gradual ramp-up of WebSocket connections
        websocket_load: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '30s', target: 50 },   // Ramp up to 50 connections
                { duration: '1m', target: 100 },   // Ramp up to 100 connections
                { duration: '2m', target: 100 },   // Hold at 100 connections
                { duration: '30s', target: 200 },  // Spike to 200 connections
                { duration: '1m', target: 200 },   // Hold at 200 connections
                { duration: '30s', target: 0 },    // Ramp down
            ],
            gracefulRampDown: '10s',
        },

        // Constant rate of new connections (stress test)
        connection_stress: {
            executor: 'constant-arrival-rate',
            rate: 10,                    // 10 new connections per second
            timeUnit: '1s',
            duration: '2m',
            preAllocatedVUs: 50,
            maxVUs: 200,
            startTime: '5m',             // Start after main load test
        },
    },
    thresholds: {
        'ws_connection_success_rate': ['rate>0.95'],    // 95% connection success
        'ws_connection_duration': ['p(95)<5000'],       // 95% connect within 5s
        'ws_message_latency': ['p(95)<1000'],           // 95% message latency under 1s
    },
};

// Generate a mock JWT token (for testing purposes)
function generateToken(userId) {
    // In a real test, you would either:
    // 1. Pre-generate tokens before the test
    // 2. Call an auth endpoint to get tokens
    // 3. Use a shared secret to generate tokens

    // This is a simplified token for testing
    // The gateway should be configured to accept these in test mode
    const header = { alg: 'HS256', typ: 'JWT' };
    const payload = {
        sub: userId,
        iss: 'banking-auth-service',
        aud: 'banking-websocket-gateway',
        exp: Math.floor(Date.now() / 1000) + 3600,
        iat: Math.floor(Date.now() / 1000),
    };

    // Note: k6 doesn't have native JWT signing, so in production tests
    // you would pre-generate tokens or use a token endpoint
    // For this example, we'll use a placeholder
    return `test-token-${userId}`;
}

// Health check before starting load test
export function setup() {
    console.log('Checking gateway health...');

    const healthRes = http.get(`${HTTP_URL}/health`);
    check(healthRes, {
        'health check passed': (r) => r.status === 200,
    });

    const readyRes = http.get(`${HTTP_URL}/ready`);
    check(readyRes, {
        'ready check passed': (r) => r.status === 200,
    });

    if (healthRes.status !== 200 || readyRes.status !== 200) {
        console.error('Gateway not healthy, aborting test');
        throw new Error('Gateway health check failed');
    }

    console.log('Gateway is healthy, starting load test');
    return { startTime: Date.now() };
}

// Main test function
export default function (data) {
    const userId = `load-test-user-${__VU}-${__ITER}`;
    const token = generateToken(userId);
    const url = `${WS_URL}?token=${token}`;

    const connectionStart = Date.now();

    const res = ws.connect(url, {}, function (socket) {
        wsConnections.add(1);
        wsConnectionRate.add(true);
        wsConnectionDuration.add(Date.now() - connectionStart);

        let messageCount = 0;
        let lastPingTime = 0;

        socket.on('open', function () {
            console.log(`VU ${__VU}: Connected as ${userId}`);

            // Send initial ping
            lastPingTime = Date.now();
            socket.send(JSON.stringify({
                type: 'ping',
                requestId: `ping-${__VU}-${Date.now()}`,
            }));
            wsMessagesSent.add(1);
        });

        socket.on('message', function (message) {
            wsMessagesReceived.add(1);
            messageCount++;

            try {
                const data = JSON.parse(message);

                if (data.type === 'pong') {
                    const latency = Date.now() - lastPingTime;
                    wsMessageLatency.add(latency);
                }

                // Check for server messages
                if (data.type === 'notification' || data.type === 'transfer_completed') {
                    console.log(`VU ${__VU}: Received ${data.type}`);
                }
            } catch (e) {
                console.error(`VU ${__VU}: Failed to parse message: ${e}`);
            }
        });

        socket.on('close', function () {
            console.log(`VU ${__VU}: Connection closed after ${Date.now() - connectionStart}ms, ${messageCount} messages`);
        });

        socket.on('error', function (e) {
            console.error(`VU ${__VU}: WebSocket error: ${e}`);
        });

        // Stay connected and periodically send pings
        const connectionDuration = Math.random() * 30000 + 30000; // 30-60 seconds
        const pingInterval = 5000; // Every 5 seconds

        let elapsed = 0;
        while (elapsed < connectionDuration) {
            sleep(pingInterval / 1000);
            elapsed += pingInterval;

            if (socket.readyState === 1) { // OPEN
                lastPingTime = Date.now();
                socket.send(JSON.stringify({
                    type: 'ping',
                    requestId: `ping-${__VU}-${Date.now()}`,
                }));
                wsMessagesSent.add(1);
            }
        }

        socket.close();
    });

    if (res === null) {
        wsConnectionsFailed.add(1);
        wsConnectionRate.add(false);
        console.error(`VU ${__VU}: Failed to connect`);
    }

    // Brief pause before next iteration
    sleep(Math.random() * 2 + 1);
}

// Cleanup and reporting
export function teardown(data) {
    console.log('Load test completed');
    console.log(`Total duration: ${(Date.now() - data.startTime) / 1000}s`);

    // Check final metrics
    const statusRes = http.get(`${HTTP_URL}/status`);
    if (statusRes.status === 200) {
        const status = JSON.parse(statusRes.body);
        console.log(`Final connection count: ${status.connections?.totalConnections || 0}`);
        console.log(`Active users: ${status.connections?.activeUsers || 0}`);
    }
}

// Rate limit test scenario
export function rateLimitTest() {
    const userId = `rate-limit-test-${__VU}`;

    // Try to establish many connections rapidly
    for (let i = 0; i < 20; i++) {
        const token = generateToken(`${userId}-${i}`);
        const res = ws.connect(`${WS_URL}?token=${token}`, {}, function (socket) {
            socket.on('open', function () {
                socket.close();
            });
        });

        if (res === null) {
            console.log(`Rate limit test: Connection ${i} was rejected (expected after limit)`);
        }

        // No delay - try to trigger rate limiting
    }
}
