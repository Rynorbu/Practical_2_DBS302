import redis
import sys
from typing import Optional
from datetime import datetime


class RealtimeAnalytics:
    """
    Real-time analytics using Redis bitmaps and HyperLogLog.
    """

    def __init__(self, redis_client: Optional[redis.Redis] = None) -> None:
        self.r = redis_client or redis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            decode_responses=True,
        )

    def _dau_key(self, date_str: str) -> str:
        return f"analytics:dau:{date_str}"

    def _uv_key(self, date_str: str) -> str:
        return f"analytics:uv:{date_str}"

    def mark_user_active(self, date_str: str, user_id: int) -> None:
        if user_id < 0:
            raise ValueError("user_id must be non-negative")
        self.r.setbit(self._dau_key(date_str), user_id, 1)

    def add_visit(self, date_str: str, user_identifier: str) -> None:
        self.r.pfadd(self._uv_key(date_str), user_identifier)

    def count_daily_active_users(self, date_str: str) -> int:
        return self.r.bitcount(self._dau_key(date_str))

    def count_unique_visitors(self, date_str: str) -> int:
        return self.r.pfcount(self._uv_key(date_str))


def validate_date(date_str: str) -> bool:
    """
    Validate that the date string is in YYYY-MM-DD format.
    """
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def seed_data(analytics: RealtimeAnalytics, date_str: str) -> None:
    """
    Seed some sample data for the given date so the CLI has something to show.
    """
    analytics.mark_user_active(date_str, 1)
    analytics.mark_user_active(date_str, 42)
    analytics.mark_user_active(date_str, 100)
    analytics.mark_user_active(date_str, 200)
    analytics.mark_user_active(date_str, 305)

    analytics.add_visit(date_str, "user1")
    analytics.add_visit(date_str, "user2")
    analytics.add_visit(date_str, "user3")
    analytics.add_visit(date_str, "user2")   # duplicate
    analytics.add_visit(date_str, "user4")
    analytics.add_visit(date_str, "user5")
    analytics.add_visit(date_str, "user3")   # duplicate
    analytics.add_visit(date_str, "user6")


def main():
    # ── Validate argument ──────────────────────────────
    if len(sys.argv) != 2:
        print("Usage: python cli_analytics.py <YYYY-MM-DD>")
        print("Example: python cli_analytics.py 2026-03-24")
        sys.exit(1)

    date_str = sys.argv[1]

    if not validate_date(date_str):
        print(f"Error: '{date_str}' is not a valid date. Use format YYYY-MM-DD.")
        print("Example: python cli_analytics.py 2026-03-24")
        sys.exit(1)

    # ── Connect and seed ───────────────────────────────
    try:
        analytics = RealtimeAnalytics()
        # Seed sample data so there is something to display
        seed_data(analytics, date_str)
    except Exception as e:
        print(f"Error: Could not connect to Redis. Is it running?")
        print(f"Details: {e}")
        sys.exit(1)

    # ── Fetch metrics ──────────────────────────────────
    dau = analytics.count_daily_active_users(date_str)
    uv  = analytics.count_unique_visitors(date_str)

    # ── Print report ───────────────────────────────────
    print("=" * 40)
    print(f"  Analytics Report for {date_str}")
    print("=" * 40)
    print(f"  DAU (Daily Active Users) : {dau}  [exact]")
    print(f"  UV  (Unique Visitors)    : {uv}  [approx]")
    print("=" * 40)


if __name__ == "__main__":
    main()