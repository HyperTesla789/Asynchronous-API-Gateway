# Asynchronous API Gateway & Router

A production-grade, distributed systems middleware layer designed to coordinate microservice communication, handle multi-tenant traffic shaping, and isolate downstream cluster failures.

## Core Architectural Impact

* Architected asynchronous API gateway
* Built using Python and `asyncio`
* Implemented non-blocking network I/O
* Engineered dynamic reverse-proxy routing
* Developed custom Token Bucket rate-limiter
* Mitigated DDoS and resource exhaustion
* Integrated Redis in-memory state store
* Designed automated Circuit Breaker pattern
* Isolated failing downstream dependencies
* Prevented cascading system failures
* Tracked end-to-end request latency
* Structured JSON log streams
* Configured graceful degradation fallback paths

## System Flow & Middleware Architecture

1. **Ingress Layer:** Captures concurrent client sockets.
2. **Rate Limiter (Token Bucket):** Evaluates tenant IP capacity to drop abusive connections with HTTP 429.
3. **State-Driven Circuit Breaker:** Monitors downstream microservice health trends. If failure thresholds breach, trips to `OPEN` to shield the ecosystem with HTTP 503 fallback routing.
4. **Caching Engine (Mock Redis):** Performs asynchronous in-memory lookups to eliminate redundant network I/O overhead.

## Quick Start & Verification

This project runs completely out of the box with zero external dependencies.

```bash
# Clone the repository
git clone [https://github.com/YOUR_USERNAME/Asynchronous-API-Gateway.git](https://github.com/YOUR_USERNAME/Asynchronous-API-Gateway.git)
cd Asynchronous-API-Gateway

# Execute the concurrent cluster simulation
python3 gateway.py
