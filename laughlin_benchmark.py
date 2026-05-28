cat << 'EOF' > laughlin_benchmark.py
import asyncio
import time
import random
from collections import deque
from typing import Dict, Any, List
from laughlin_paate_correlator import LaughlinPAATECorrelator

class StandardDequeCorrelator:
    def __init__(self, jitter_tolerance: float = 0.005, max_buffer_size: int = 100000):
        self.tolerance = jitter_tolerance
        self.max_buffer_size = max_buffer_size
        self.buffer_a = deque()
        self.buffer_b = deque()

    def process_event_a(self, event_a: Dict[str, Any]) -> List[Dict[str, Any]]:
        t_a = event_a["time"]
        self.buffer_a.append(event_a)
        if len(self.buffer_a) > self.max_buffer_size:
            self.buffer_a.popleft()
        
        results = []
        low, high = t_a - self.tolerance, t_a + self.tolerance
        for item in self.buffer_b:
            if low <= item["time"] <= high:
                results.append({"match_time_A": t_a, "matching_events_B": [{"time": item["time"]}]})
        return results

    def process_event_b(self, event_b: Dict[str, Any]) -> None:
        self.buffer_b.append(event_b)
        if len(self.buffer_b) > self.max_buffer_size:
            self.buffer_b.popleft()

async def run_benchmark():
    print("=" * 60)
    print("STARTING LIVE BENCHMARK: LaughlinPAATE vs Standard Deque")
    print("=" * 60)
    
    num_events = 20000
    print(f"Generating {num_events} randomized stream events with jitter...")
    
    base_time = 1000.0
    stream_a = [{"time": base_time + (i * 0.01) + random.uniform(-0.02, 0.02), "id": i} for i in range(num_events)]
    stream_b = [{"time": base_time + (i * 0.01) + random.uniform(-0.02, 0.02), "id": i} for i in range(num_events)]
    
    stream_a.sort(key=lambda x: x["time"])
    stream_b.sort(key=lambda x: x["time"])

    print("\n[Phase 1] Testing Standard Deque Engine...")
    std_correlator = StandardDequeCorrelator(max_buffer_size=num_events)
    
    for ev_b in stream_b:
        std_correlator.process_event_b(ev_b)
        
    start_std = time.perf_counter()
    std_matches = 0
    for ev_a in stream_a:
        res = std_correlator.process_event_a(ev_a)
        std_matches += len(res)
    end_std = time.perf_counter()
    std_time = (end_std - start_std) * 1000

    print(f"  -> Done. Total time: {std_time:.2f} ms | Total matches found: {std_matches}")

    print("\n[Phase 2] Testing LaughlinPAATECorrelator Engine...")
    laughlin_correlator = LaughlinPAATECorrelator(max_buffer_size=num_events)
    
    for ev_b in stream_b:
        await laughlin_correlator.process_event_b(ev_b)
        
    start_laughlin = time.perf_counter()
    laughlin_matches = 0
    for ev_a in stream_a:
        res = await laughlin_correlator.process_event_a(ev_a)
        laughlin_matches += len(res)
    end_laughlin = time.perf_counter()
    laughlin_time = (end_laughlin - start_laughlin) * 1000

    print(f"  -> Done. Total time: {laughlin_time:.2f} ms | Total matches found: {laughlin_matches}")

    print("\n" + "=" * 60)
    print("BENCHMARK RAW DATA FOR USER DEFINITION:")
    print(f"STD_TIME_MS={std_time:.4f}")
    print(f"LAUGHLIN_TIME_MS={laughlin_time:.4f}")
    print(f"MATCHES_VERIFIED={laughlin_matches == std_matches}")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(run_benchmark())
EOF
python3 laughlin_benchmark.py
