import redis
from typing import Optional


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
        """
        Build key for daily active user bitmap.
        Example date_str: '2026-03-17'.
        """
        return f"analytics:dau:{date_str}"

    def mark_user_active(self, date_str: str, user_id: int) -> None:
        """
        Mark a user as active for a given day using SETBIT.
        """
        if user_id < 0:
            raise ValueError("user_id must be non-negative")

        key = self._dau_key(date_str)
        # Set bit at position = user_id
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
        """
        Build key for daily unique visitors HyperLogLog.
        """
        return f"analytics:uv:{date_str}"

    def add_visit(self, date_str: str, user_identifier: str) -> None:
        """
        Add a visit for a given day in HyperLogLog.
        user_identifier can be a user_id, session_id, or IP string.
        """
        key = self._uv_key(date_str)
        self.r.pfadd(key, user_identifier)

    def count_unique_visitors(self, date_str: str) -> int:
        """
        Get approximate number of unique visitors for the given date using PFCOUNT.
        """
        key = self._uv_key(date_str)
        return self.r.pfcount(key)


def demo():
    analytics = RealtimeAnalytics()

    date = "2026-03-17"

    # Clear previous demo data
    analytics.r.delete(analytics._dau_key(date))
    analytics.r.delete(analytics._uv_key(date))

    # Simulate some activity
    print(f"Simulating activity for {date}...")

    # Users 1, 42, 100 are active
    analytics.mark_user_active(date, 1)
    analytics.mark_user_active(date, 42)
    analytics.mark_user_active(date, 100)

    # Visits (note repeated identifiers)
    analytics.add_visit(date, "user1")
    analytics.add_visit(date, "user2")
    analytics.add_visit(date, "user3")
    analytics.add_visit(date, "user2")  # duplicate
    analytics.add_visit(date, "user3")  # duplicate
    analytics.add_visit(date, "user4")

    # Check a user's activity
    print("\nIs user 42 active?")
    print(" ->", analytics.is_user_active(date, 42))

    # Daily active users (exact, from bitmap)
    dau = analytics.count_daily_active_users(date)
    print("\nDaily Active Users (DAU):", dau)

    # Unique visitors (approximate, from HyperLogLog)
    uv = analytics.count_unique_visitors(date)
    print("Unique Visitors (UV) [approx]:", uv)


if __name__ == "__main__":
    demo()
