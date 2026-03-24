import redis
from datetime import datetime, timedelta
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
        return f"analytics:active:{date_str}"

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

    # ----------------------------
    # New: Merging & Stickiness
    # ----------------------------
    def merge_uv_period(self, label: str, date_str_list: List[str]) -> int:
        """
        Exercise 1: Merges daily HLL keys into a temporary period key.
        """
        merged_key = f"analytics:uv:merged:{label}"
        source_keys = [self._uv_key(d) for d in date_str_list]
        
        self.r.pfmerge(merged_key, *source_keys)
        self.r.expire(merged_key, 3600)  # Cleanup after 1 hour
        return self.r.pfcount(merged_key)

    def compute_stickiness(self, target_date: str) -> float:
        """
        Exercise 2: DAU / MAU ratio. 
        MAU is derived by merging the last 30 daily HLL keys.
        """
        # 1. Get exact DAU from Bitmap
        dau = self.count_daily_active_users(target_date)
        
        # 2. Generate last 30 days and merge HLLs for MAU
        end_dt = datetime.strptime(target_date, "%Y-%m-%d")
        last_30_days = [(end_dt - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(30)]
        mau = self.merge_uv_period(f"mau:{target_date}", last_30_days)
        
        if mau == 0:
            return 0.0
            
        return round((dau / mau) * 100, 2)


def demo():
    analytics = RealtimeAnalytics()
    date = "2026-03-17"

    # Reset
    analytics.r.delete(analytics._dau_key(date), analytics._uv_key(date))

    # Activity
    analytics.mark_user_active(date, 42)
    analytics.add_visit(date, "user42")
    analytics.add_visit(date, "user99") # Someone who didn't "log in" but visited

    print(f"Stats for {date}:")
    print(f"DAU: {analytics.count_daily_active_users(date)}")
    print(f"Stickiness: {analytics.compute_stickiness(date)}%")

if __name__ == "__main__":
    demo()
