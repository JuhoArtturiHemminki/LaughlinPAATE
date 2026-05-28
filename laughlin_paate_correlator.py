import asyncio
import time
import bisect
from typing import Dict, Any, List, Tuple, Set

class LaughlinPAATECorrelator:
    def __init__(
        self, 
        jitter_tolerance: float = 0.005, 
        grace_period: float = 2.0,
        max_buffer_size: int = 100000, 
        max_idle_seconds: float = 10.0
    ):
        self.tolerance: float = jitter_tolerance
        self.grace_period: float = grace_period
        self.max_buffer_size: int = max_buffer_size
        self.max_idle_seconds: float = max_idle_seconds
        
        self.lock = asyncio.Lock()
        
        self.buffer_a: List[Tuple[float, Dict[str, Any], float]] = []
        self.buffer_b: List[Tuple[float, Dict[str, Any], float]] = []
        
        self.offset_a: int = 0
        self.offset_b: int = 0
        
        self.reported_matches: List[Tuple[float, Tuple[float, float]]] = []
        self.reported_set: Set[Tuple[float, float]] = set()
        self.offset_matches: int = 0
        
        self.latest_time_a: float = 0.0
        self.latest_time_b: float = 0.0

    async def process_event_a(self, event_a: Dict[str, Any]) -> List[Dict[str, Any]]:
        time_a = event_a.get("time")
        if time_a is None:
            return []

        async with self.lock:
            current_wall_time = time.time()
            self.latest_time_a = max(self.latest_time_a, time_a)
            
            new_item = (time_a, event_a, current_wall_time)
            self.buffer_a.append(new_item)
            
            idx = len(self.buffer_a) - 1
            while idx > self.offset_a and self.buffer_a[idx - 1][0] > time_a:
                self.buffer_a[idx], self.buffer_a[idx - 1] = self.buffer_a[idx - 1], self.buffer_a[idx]
                idx -= 1
                
            matches = self._find_matches(time_a, event_a, self.buffer_b, self.offset_b, is_trigger_a=True)
            self._clean_buffers(current_wall_time)
            return matches

    async def process_event_b(self, event_b: Dict[str, Any]) -> List[Dict[str, Any]]:
        time_b = event_b.get("time")
        if time_b is None:
            return []

        async with self.lock:
            current_wall_time = time.time()
            self.latest_time_b = max(self.latest_time_b, time_b)
            
            new_item = (time_b, event_b, current_wall_time)
            self.buffer_b.append(new_item)
            
            idx = len(self.buffer_b) - 1
            while idx > self.offset_b and self.buffer_b[idx - 1][0] > time_b:
                self.buffer_b[idx], self.buffer_b[idx - 1] = self.buffer_b[idx - 1], self.buffer_b[idx]
                idx -= 1
                
            matches = self._find_matches(time_b, event_b, self.buffer_a, self.offset_a, is_trigger_a=False)
            self._clean_buffers(current_wall_time)
            return matches

    def _find_matches(self, trigger_time: float, trigger_event: Dict[str, Any], target_buffer: List[Tuple[float, Dict[str, Any], float]], offset: int, is_trigger_a: bool) -> List[Dict[str, Any]]:
        if len(target_buffer) <= offset:
            return []

        lower_bound = trigger_time - self.tolerance
        upper_bound = trigger_time + self.tolerance
        results = []
        current_wall_time = time.time()

        dummy_target = (lower_bound, {}, 0.0)
        start_idx = bisect.bisect_left(target_buffer, dummy_target, lo=offset)

        for i in range(start_idx, len(target_buffer)):
            item_time, item, _ = target_buffer[i]
            
            if item_time > upper_bound:
                break
            
            match_key = (trigger_time, item_time) if is_trigger_a else (item_time, trigger_time)
            
            if match_key in self.reported_set:
                continue
                
            self.reported_set.add(match_key)
            self.reported_matches.append((current_wall_time, match_key))
            
            event_a = trigger_event if is_trigger_a else item
            event_b = item if is_trigger_a else trigger_event
            
            results.append({
                "match_time_A": event_a["time"],
                "metadata_A": event_a.get("metadata", {}),
                "matching_events_B": [{"time": event_b["time"], "meta": event_b.get("metadata", {})}]
            })
        return results

    def _clean_buffers(self, current_wall_time: float) -> None:
        cutoff_a = self.latest_time_b - self.tolerance - self.grace_period
        cutoff_b = self.latest_time_a - self.tolerance - self.grace_period

        dummy_a_time = (cutoff_a, {}, 0.0)
        idx_a_time = bisect.bisect_right(self.buffer_a, dummy_a_time, lo=self.offset_a)
        
        clean_up_to_a = idx_a_time
        for i in range(self.offset_a, len(self.buffer_a)):
            if current_wall_time - self.buffer_a[i][2] > self.max_idle_seconds:
                clean_up_to_a = max(clean_up_to_a, i + 1)
            else:
                break
                
        active_len_a = len(self.buffer_a) - clean_up_to_a
        if active_len_a > self.max_buffer_size:
            clean_up_to_a = len(self.buffer_a) - self.max_buffer_size
            
        if clean_up_to_a > self.offset_a:
            self.offset_a = clean_up_to_a

        dummy_b_time = (cutoff_b, {}, 0.0)
        idx_b_time = bisect.bisect_right(self.buffer_b, dummy_b_time, lo=self.offset_b)
        
        clean_up_to_b = idx_b_time
        for i in range(self.offset_b, len(self.buffer_b)):
            if current_wall_time - self.buffer_b[i][2] > self.max_idle_seconds:
                clean_up_to_b = max(clean_up_to_b, i + 1)
            else:
                break
                
        active_len_b = len(self.buffer_b) - clean_up_to_b
        if active_len_b > self.max_buffer_size:
            clean_up_to_b = len(self.buffer_b) - self.max_buffer_size
            
        if clean_up_to_b > self.offset_b:
            self.offset_b = clean_up_to_b

        dummy_match_idle = (current_wall_time - self.max_idle_seconds, (0.0, 0.0))
        idx_match_idle = bisect.bisect_left(self.reported_matches, dummy_match_idle, lo=self.offset_matches)
        if idx_match_idle > self.offset_matches:
            for i in range(self.offset_matches, idx_match_idle):
                self.reported_set.discard(self.reported_matches[i][1])
            self.offset_matches = idx_match_idle

        gc_threshold = max(1000, self.max_buffer_size // 2)
        
        if self.offset_a > gc_threshold:
            self.buffer_a = self.buffer_a[self.offset_a:]
            self.offset_a = 0
            
        if self.offset_b > gc_threshold:
            self.buffer_b = self.buffer_b[self.offset_b:]
            self.offset_b = 0

        if self.offset_matches > gc_threshold:
            self.reported_matches = self.reported_matches[self.offset_matches:]
            self.offset_matches = 0
