import asyncio
import time
import random
import json
from dataclasses import dataclass
from typing import Dict, List, Tuple

# ============================================================================
# LAYER 1: ARCHITECTURAL STATE DEFINITIONS & METRICS
# ============================================================================

@dataclass
class GatewayMetrics:
    total_requests: int = 0
    successful_routes: int = 0
    rate_limited: int = 0
    circuit_tripped: int = 0
    failed_backends: int = 0
    total_latency_ms: float = 0.0

# ============================================================================
# LAYER 2: SYSTEM MIDDLEWARE COMPONENTS
# ============================================================================

class MockRedisCache:
    """Simulates a highly concurrent, central Redis key-value store for global state."""
    def __init__(self):
        self._storage: Dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> str | None:
        async with self._lock:
            return self._storage.get(key, None)

    async def set(self, key: str, value: str, expire_secs: int = 0):
        async with self._lock:
            self._storage[key] = value
            if expire_secs > 0:
                asyncio.create_task(self._expire_task(key, expire_secs))

    async def _expire_task(self, key: str, delay: int):
        await asyncio.sleep(delay)
        async with self._lock:
            self._storage.pop(key, None)


class TokenBucketRateLimiter:
    """Implements enterprise multi-tenant rate limiting via token bucket algorithm."""
    def __init__(self, capacity: int, refill_rate_per_sec: int):
        self.capacity = capacity
        self.refill_rate = refill_rate_per_sec
        self.buckets: Dict[str, Tuple[float, float]] = {} # tenant_id -> (tokens, last_refill_time)
        self._lock = asyncio.Lock()

    async def allow_request(self, client_ip: str) -> bool:
        async with self._lock:
            now = time.time()
            if client_ip not in self.buckets:
                self.buckets[client_ip] = (float(self.capacity), now)
                return True

            tokens, last_refill = self.buckets[client_ip]
            # Calculate tokens gained since last execution window
            elapsed = now - last_refill
            refilled_tokens = tokens + (elapsed * self.refill_rate)
            current_tokens = min(float(self.capacity), refilled_tokens)

            if current_tokens >= 1.0:
                self.buckets[client_ip] = (current_tokens - 1.0, now)
                return True
            
            # Sub-one token status flags a 429 Rate Limit Breach
            self.buckets[client_ip] = (current_tokens, now)
            return False


class CircuitBreaker:
    """Implements a finite state machine circuit breaker to stop cascading cluster failures."""
    def __init__(self, failure_threshold: int, recovery_time_secs: int):
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time_secs
        self.state = "CLOSED" # CLOSED, OPEN, HALF-OPEN
        self.failure_count = 0
        self.last_state_change = time.time()
        self._lock = asyncio.Lock()

    async def can_execute(self) -> bool:
        async with self._lock:
            now = time.time()
            if self.state == "OPEN":
                if now - self.last_state_change > self.recovery_time:
                    self.state = "HALF-OPEN"
                    self.last_state_change = now
                    print(f"⚠️ [CIRCUIT BREAKER] Entering HALF-OPEN state. Testing backend health...")
                    return True
                return False
            return True

    async def record_success(self):
        async with self._lock:
            self.failure_count = 0
            if self.state == "HALF-OPEN":
                self.state = "CLOSED"
                self.last_state_change = time.time()
                print(f"✅ [CIRCUIT BREAKER] Resetting to CLOSED. Downstream services completely recovered.")

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            now = time.time()
            if self.state in ("CLOSED", "HALF-OPEN") and self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                self.last_state_change = now
                print(f"🚨 [CIRCUIT BREAKER] Tripped to OPEN! Failure threshold reached. Diverting traffic.")

# ============================================================================
# LAYER 3: CORE API GATEWAY ROUTER ENGINE
# ============================================================================

class AsynchronousApiGateway:
    def __init__(self):
        self.rate_limiter = TokenBucketRateLimiter(capacity=5, refill_rate_per_sec=2)
        self.circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_time_secs=3)
        self.cache = MockRedisCache()
        self.metrics = GatewayMetrics()
        
        # Route map to backend microservices
        self.routing_table = {
            "/checkout": ["https://checkout-srv-1", "https://checkout-srv-2"],
            "/inventory": ["https://inventory-srv-1"]
        }

    async def handle_request(self, path: str, client_ip: str, payload: dict) -> Tuple[int, str]:
        start_time = time.time()
        self.metrics.total_requests += 1

        # Execution Step 1: Enforce Global Security / Rate Limiting
        if not await self.rate_limiter.allow_request(client_ip):
            self.metrics.rate_limited += 1
            return 429, json.dumps({"error": "Too Many Requests. Rate limit exceeded."})

        # Execution Step 2: Route Resolution
        if path not in self.routing_table:
            return 404, json.dumps({"error": "Gateway Route Not Found"})

        # Execution Step 3: Circuit Breaker Evaluation
        if not await self.circuit_breaker.can_execute():
            self.metrics.circuit_tripped += 1
            return 503, json.dumps({"error": "Service Temporarily Unavailable: Circuit Breaker Active"})

        # Execution Step 4: Distributed Cache Lookup (Prior to Network I/O)
        cache_key = f"cache:{path}:{hash(frozenset(payload.items()))}"
        cached_response = await self.cache.get(cache_key)
        if cached_response:
            self.metrics.successful_routes += 1
            return 200, cached_response

        # Execution Step 5: Asynchronous Reverse-Proxy Routing to Microservice
        target_backend = random.choice(self.routing_table[path])
        try:
            status, response_body = await self._forward_network_io(target_backend, payload)
            
            if status == 200:
                await self.circuit_breaker.record_success()
                await self.cache.set(cache_key, response_body, expire_secs=5)
                self.metrics.successful_routes += 1
            else:
                await self.circuit_breaker.record_failure()
                self.metrics.failed_backends += 1
                
            return status, response_body

        except Exception:
            await self.circuit_breaker.record_failure()
            self.metrics.failed_backends += 1
            return 500, json.dumps({"error": "Internal Gateway Routing Network Exception"})
        
        finally:
            latency = (time.time() - start_time) * 1000
            self.metrics.total_latency_ms += latency

    async def _forward_network_io(self, target_host: str, payload: dict) -> Tuple[int, str]:
        """Simulates non-blocking asynchronous network sockets to downstream services."""
        # Intentionally inject structural mock failure variables to demonstrate the circuit breaker
        if "trigger_fault" in payload and payload["trigger_fault"]:
            await asyncio.sleep(0.05) # Mimic minor network delay
            return 500, json.dumps({"error": "Downstream Microservice Internal Error"})

        await asyncio.sleep(random.uniform(0.01, 0.04)) # Realistic networking delay spectrum
        return 200, json.dumps({"status": "Success", "dispatched_node": target_host, "payload_echo": payload})

# ============================================================================
# LAYER 4: MULTI-TENANT CONCURRENT SIMULATION TRACK
# ============================================================================

async def run_client_simulation():
    gateway = AsynchronousApiGateway()
    print("====================================================================")
    print("BOOTING ASYNCHRONOUS API GATEWAY ENGINE & ROUTER CLUSTER")
    print("====================================================================\n")

    # Scenario A: Standard High-Volume Clean Traffic Processing
    print("⚡ Stage 1: Dispatched 10 Concurrent Normalized Network Sockets...")
    tasks = [
        gateway.handle_request("/checkout", "192.168.1.50", {"item": "milk", "qty": i})
        for i in range(10)
    ]
    results = await asyncio.gather(*tasks)
    print(f"-> Completed Stage 1. Rate Limiting status: {gateway.metrics.rate_limited} dropped connections.\n")

    # Scenario B: Target Microservice Degradation (Tripping the Circuit Breaker)
    print("💥 Stage 2: Simulating Downstream Microservice Meltdown (Injecting Faults)...")
    fault_tasks = [
        gateway.handle_request("/checkout", "10.0.0.1", {"trigger_fault": True})
        for _ in range(4)
    ]
    await asyncio.gather(*fault_tasks)
    print(f"-> Current Gateway State Flag: Circuit Breaker Status is now: [{gateway.metrics.circuit_tripped > 0 and 'OPEN' or 'TRACKING'}]\n")

    # Scenario C: Circuit Verification during Active Lockdown
    print("🛡️ Stage 3: Routing clean requests to /checkout while Circuit is OPEN...")
    status, body = await gateway.handle_request("/checkout", "192.168.10.12", {"item": "bread"})
    print(f"-> Gateway protected isolation rule result: Status Code {status} | Body: {body}\n")

    # Scenario D: System Cooldown and Half-Open Verification
    print("⏳ Stage 4: Halting traffic for 3.5 seconds to cool down the breaker state...")
    await asyncio.sleep(3.5)
    
    print("\n🔄 Stage 5: Dispatched test verification probe to clean backend path...")
    status, body = await gateway.handle_request("/checkout", "192.168.10.12", {"item": "recovered_checkout"})
    print(f"-> Probe Result: Status Code {status} | Body: {body}\n")

    # Final Engineering Analytics Report Generation
    print("====================================================================")
    print("               FINAL RUNTIME METRICS ARCHITECTURE REPORT            ")
    print("====================================================================")
    print(f" Total Aggregated Gateway Ingress Requests : {gateway.metrics.total_requests}")
    print(f" Fully Completed Backend Routes             : {gateway.metrics.successful_routes}")
    print(f" Over-Capacity Dropped Requests (429)       : {gateway.metrics.rate_limited}")
    print(f" Shielded/Circuit Blocked Requests (503)    : {gateway.metrics.circuit_tripped}")
    print(f" Isolated Downstream Node Failures          : {gateway.metrics.failed_backends}")
    print(f" Mean Processing Turn-around Latency       : {(gateway.metrics.total_latency_ms / gateway.metrics.total_requests):.2f} ms")
    print("====================================================================")

if __name__ == "__main__":
    asyncio.run(run_client_simulation())
