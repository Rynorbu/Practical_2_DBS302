import redis
from typing import Optional, List
from datetime import datetime, timedelta


class RealtimeAnalytics:
    """
    Real-time analytics using Redis bitmaps and HyperLogLog.
    Tracks:
      - Daily Active Users (DAU) via bitmaps.
      - Daily Unique Visitors (UV) via HyperLogLog.
      - Monthly Active Users (MAU) via merging 30 daily HLL keys.
      - Stickiness ratio = DAU / MAU.
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
        return self.r.getbit(key, user_id) == 1

    def count_daily_active_users(self, date_str: str) -> int:
        """
        Count daily active users using BITCOUNT.
        """
        key = self._dau_key(date_str)
        return self.r.bitcount(key)

    # ------------------------
    # HyperLogLog-based metrics
    # ------------------------
    def _uv_key(self, date_str: str) -> str:
        return f"analytics:uv:{date_str}"

    def _mau_key(self, year_month: str) -> str:
        """
        Build key for monthly active users HyperLogLog.
        Example year_month: '2026-03'
        """
        return f"analytics:mau:{year_month}"

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

    def merge_uv(self, date_str_list: List[str], dest_key: str) -> int:
        """
        Merge multiple daily HyperLogLog UV keys into one destination key.
        Returns the approximate unique visitor count for the merged period.
        """
        source_keys = [self._uv_key(date_str) for date_str in date_str_list]
        self.r.pfmerge(dest_key, *source_keys)
        return self.r.pfcount(dest_key)

    # ------------------------
    # MAU — Monthly Active Users
    # ------------------------
    def compute_mau(self, year_month: str) -> int:
        """
        Compute Monthly Active Users (MAU) by merging all 30 daily
        HyperLogLog UV keys for the given month into one MAU key.

        year_month example: '2026-03'
        Returns approximate unique users for the month.
        """
        # Build list of all dates in the month (up to today)
        today = datetime.utcnow().date()
        year, month = map(int, year_month.split("-"))

        # Generate all days in the month up to today
        date_list = []
        day = datetime(year, month, 1).date()
        while day.month == month and day <= today:
            date_list.append(day.strftime("%Y-%m-%d"))
            day += timedelta(days=1)

        if not date_list:
            return 0

        # Merge all daily UV keys into MAU key
        mau_key = self._mau_key(year_month)
        source_keys = [self._uv_key(d) for d in date_list]
        self.r.pfmerge(mau_key, *source_keys)

        return self.r.pfcount(mau_key)

    # ------------------------
    # Stickiness = DAU / MAU
    # ------------------------
    def compute_stickiness(self, date_str: str) -> dict:
        """
        Compute stickiness ratio = DAU / MAU.

        date_str example: '2026-03-24'
        Returns a dict with dau, mau, and stickiness ratio.
        """
        # Get DAU from bitmap (exact)
        dau = self.count_daily_active_users(date_str)

        # Derive year_month from date_str
        year_month = date_str[:7]  # '2026-03-24' -> '2026-03'

        # Get MAU from HyperLogLog (approximate)
        mau = self.compute_mau(year_month)

        # Compute stickiness
        if mau == 0:
            stickiness = 0.0
        else:
            stickiness = round(dau / mau, 4)

        return {
            "date": date_str,
            "dau": dau,
            "mau": mau,
            "stickiness": stickiness,
            "stickiness_pct": f"{stickiness * 100:.2f}%",
        }


def demo():
    analytics = RealtimeAnalytics()

    # ── SETUP: Simulate a full month of data ───────────
    print("Simulating 30 days of user activity for March 2026...")

    # Generate all days in March 2026
    days = []
    day = datetime(2026, 3, 1)
    while day.month == 3:
        days.append(day.strftime("%Y-%m-%d"))
        day += timedelta(days=1)

    # Clean old data
    for d in days:
        analytics.r.delete(analytics._uv_key(d))
        analytics.r.delete(analytics._dau_key(d))
    analytics.r.delete(analytics._mau_key("2026-03"))

    # Simulate visits — 100 users spread across 30 days
    # Each day has ~20-40 active users with overlaps (realistic)
    import random
    random.seed(42)
    all_users = [f"user{i}" for i in range(1, 101)]   # 100 total users

    for d in days:
        # Pick random subset of users active that day
        daily_users = random.sample(all_users, random.randint(20, 40))
        for u in daily_users:
            analytics.add_visit(d, u)                  # HyperLogLog (UV)
            analytics.mark_user_active(d, int(u[4:]))  # Bitmap (DAU)

    # ── TODAY: March 24 ───────────────────────────────
    today = "2026-03-24"
    print(f"\nComputing stickiness for {today}...\n")

    result = analytics.compute_stickiness(today)

    print(f"  Date       : {result['date']}")
    print(f"  DAU        : {result['dau']} (exact, from bitmap)")
    print(f"  MAU        : {result['mau']} (approx, from HyperLogLog)")
    print(f"  Stickiness : {result['stickiness']} ({result['stickiness_pct']})")

    # ── INTERPRETATION ────────────────────────────────
    print("\nInterpretation:")
    s = result["stickiness"]
    if s >= 0.40:
        print(f"  Excellent! {result['stickiness_pct']} of monthly users came back today.")
    elif s >= 0.20:
        print(f"  Good. {result['stickiness_pct']} of monthly users came back today.")
    else:
        print(f"  Low. Only {result['stickiness_pct']} of monthly users came back today.")

    # ── SHOW DAILY BREAKDOWN ──────────────────────────
    print("\nDaily breakdown (first 5 days of March):")
    print(f"  {'Date':<15} {'DAU':<8} {'UV (approx)':<15}")
    print(f"  {'-'*38}")
    for d in days[:5]:
        dau = analytics.count_daily_active_users(d)
        uv = analytics.count_unique_visitors(d)
        print(f"  {d:<15} {dau:<8} {uv:<15}")


if __name__ == "__main__":
    demo()