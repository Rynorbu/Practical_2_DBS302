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
        """
        Mark a user as active for a given day using SETBIT.
        """
        if user_id < 0:
            raise ValueError("user_id must be non-negative")
        key = self._dau_key(date_str)
        self.r.setbit(key, user_id, 1)

    def is_user_active(self, date_str: str, user_id: int) -> bool:
        """
        Check whether the user was active on a given day using GETBIT.
        """
        key = self._dau_key(date_str)
        bit_value = self.r.getbit(key, user_id)
        return bit_value == 1

    def count_daily_active_users(self, date_str: str) -> int:
        """
        Count daily active users for the given date using BITCOUNT.
        """
        key = self._dau_key(date_str)
        return self.r.bitcount(key)

    # ------------------------
    # HyperLogLog-based metrics
    # ------------------------
    def _uv_key(self, date_str: str) -> str:
        return f"analytics:uv:{date_str}"

    def add_visit(self, date_str: str, user_identifier: str) -> None:
        """
        Add a visit for a given day in HyperLogLog.
        """
        key = self._uv_key(date_str)
        self.r.pfadd(key, user_identifier)

    def count_unique_visitors(self, date_str: str) -> int:
        """
        Get approximate number of unique visitors using PFCOUNT.
        """
        key = self._uv_key(date_str)
        return self.r.pfcount(key)

    # ------------------------
    # Exercise 1 — merge_uv()
    # ------------------------
    def merge_uv(self, date_str_list: List[str], dest_key: str) -> int:
        """
        Merge multiple daily HyperLogLog UV keys into one destination key.
        Uses PFMERGE to combine unique visitors across multiple days.
        Returns the approximate unique visitor count for the merged period.

        Example:
            merge_uv(["2026-03-17", "2026-03-18", "2026-03-19"], "analytics:uv:weekly:2026-W12")
        """
        # Build the list of source keys from date strings
        source_keys = [self._uv_key(date_str) for date_str in date_str_list]

        # PFMERGE dest source1 source2 source3 ...
        self.r.pfmerge(dest_key, *source_keys)

        # Return the merged unique visitor count
        return self.r.pfcount(dest_key)


def demo():
    analytics = RealtimeAnalytics()

    date = "2026-03-17"

    # Clear previous demo data
    analytics.r.delete(analytics._dau_key(date))
    analytics.r.delete(analytics._uv_key(date))

    # ── BASE DEMO ──────────────────────────────────────
    print(f"Simulating activity for {date}...")

    analytics.mark_user_active(date, 1)
    analytics.mark_user_active(date, 42)
    analytics.mark_user_active(date, 100)

    analytics.add_visit(date, "user1")
    analytics.add_visit(date, "user2")
    analytics.add_visit(date, "user3")
    analytics.add_visit(date, "user2")   # duplicate
    analytics.add_visit(date, "user3")   # duplicate
    analytics.add_visit(date, "user4")

    print("\nIs user 42 active?")
    print(" ->", analytics.is_user_active(date, 42))

    dau = analytics.count_daily_active_users(date)
    print("\nDaily Active Users (DAU):", dau)

    uv = analytics.count_unique_visitors(date)
    print("Unique Visitors (UV) [approx]:", uv)

    # ── EXERCISE 1 — merge_uv ─────────────────────────
    print("\n--- Exercise 1: merge_uv ---")

    # Simulate 3 days of visits
    days = ["2026-03-17", "2026-03-18", "2026-03-19"]

    # Clear old data
    for d in days:
        analytics.r.delete(analytics._uv_key(d))
    weekly_key = "analytics:uv:weekly:2026-W12"
    analytics.r.delete(weekly_key)

    # Day 1
    for u in ["user1", "user2", "user3"]:
        analytics.add_visit("2026-03-17", u)
    print(f"\n2026-03-17 UV: {analytics.count_unique_visitors('2026-03-17')}")

    # Day 2
    for u in ["user2", "user3", "user4", "user5"]:
        analytics.add_visit("2026-03-18", u)
    print(f"2026-03-18 UV: {analytics.count_unique_visitors('2026-03-18')}")

    # Day 3
    for u in ["user5", "user6", "user7"]:
        analytics.add_visit("2026-03-19", u)
    print(f"2026-03-19 UV: {analytics.count_unique_visitors('2026-03-19')}")

    # Merge all 3 days into weekly key
    weekly_uv = analytics.merge_uv(days, weekly_key)
    print(f"\nWeekly UV (merged, approx): {weekly_uv}")
    print(f"Weekly key used: {weekly_key}")
    print("(Expected ~7 unique users: user1 through user7)")


if __name__ == "__main__":
    demo()