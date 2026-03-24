import redis
from typing import Optional, List


class RealtimeAnalytics:
    """
    Real-time analytics using Redis bitmaps and HyperLogLog.
    Tracks:
      - Daily Active Users (DAU) via bitmaps.
      - Daily Unique Visitors (UV) via HyperLogLog.
    """

    def __init__(self, redis_client: Optional[redis.Redis] = None) -> None:
        self.r = redis_client or redis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            decode_responses=True,
        )

    # --------------------
    # Bitmap-based metrics
    # --------------------
    def _dau_key(self, date_str: str) -> str:
        return f"analytics:dau:{date_str}"

    def mark_user_active(self, date_str: str, user_id: int) -> None:
        if user_id < 0:
            raise ValueError("user_id must be non-negative")
        key = self._dau_key(date_str)
        self.r.setbit(key, user_id, 1)

    def is_user_active(self, date_str: str, user_id: int) -> bool:
        key = self._dau_key(date_str)
        bit_value = self.r.getbit(key, user_id)
        return bit_value == 1

    def count_daily_active_users(self, date_str: str) -> int:
        key = self._dau_key(date_str)
        return self.r.bitcount(key)

    # ------------------------
    # HyperLogLog-based metrics
    # ------------------------
    def _uv_key(self, date_str: str) -> str:
        return f"analytics:uv:{date_str}"

    def add_visit(self, date_str: str, user_identifier: str) -> None:
        key = self._uv_key(date_str)
        self.r.pfadd(key, user_identifier)

    def count_unique_visitors(self, date_str: str) -> int:
        key = self._uv_key(date_str)
        return self.r.pfcount(key)

    def merge_uv_period(self, destination_label: str, date_str_list: List[str]) -> int:
        """
        EXERCISE 1: Merges multiple daily HyperLogLogs into a period-based key.
        Uses PFMERGE to combine unique counts without double-counting.
        """
        merged_key = f"analytics:uv:merged:{destination_label}"
        source_keys = [self._uv_key(d) for d in date_str_list]
        
        # Merge all specified daily HLLs into one
        self.r.pfmerge(merged_key, *source_keys)
        
        # Set short TTL so the merged result doesn't waste memory permanently
        self.r.expire(merged_key, 3600) 
        
        return self.r.pfcount(merged_key)


def demo():
    analytics = RealtimeAnalytics()

    day1 = "2026-03-17"
    day2 = "2026-03-18"

    # 1. Simulate Day 1
    analytics.add_visit(day1, "user_A")
    analytics.add_visit(day1, "user_B")
    
    # 2. Simulate Day 2 (Notice user_B is a returning visitor)
    analytics.add_visit(day2, "user_B")
    analytics.add_visit(day2, "user_C")

    print(f"UV Day 1: {analytics.count_unique_visitors(day1)}") # Should be 2
    print(f"UV Day 2: {analytics.count_unique_visitors(day2)}") # Should be 2

    # 3. Merge UV for the period (Total unique visitors across both days)
    total_uv = analytics.merge_uv_period("demo-period", [day1, day2])
    
    print(f"\nMerged UV (Unique over both days): {total_uv}") 
    print("Explanation: user_B visited on both days but is only counted once.")


if __name__ == "__main__":
    demo()
