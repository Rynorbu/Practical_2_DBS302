import redis
from typing import List, Dict, Optional, Tuple
from datetime import datetime


class Leaderboard:
    """
    Leaderboard backed by Redis sorted sets.
    Supports 'daily' and 'alltime' scopes.
    """

    def __init__(
        self,
        name: str,
        scope: str = "alltime",
        date: Optional[str] = None,
        redis_client: Optional[redis.Redis] = None,
    ) -> None:
        self.name = name
        self.scope = scope

        if scope == "daily":
            day = date or datetime.utcnow().strftime("%Y-%m-%d")
            self.key = f"leaderboard:{name}:daily:{day}"
        elif scope == "alltime":
            self.key = f"leaderboard:{name}:alltime"
        else:
            raise ValueError("scope must be 'daily' or 'alltime'")

        self.r = redis_client or redis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            decode_responses=True,
        )

    def add_score(self, player_id: str, score: float) -> int:
        """
        Add or update a player's score.
        Returns the player's new rank (1-based).
        """
        self.r.zadd(self.key, {player_id: score})
        return self.get_rank(player_id)

    def increment_score(self, player_id: str, delta: float) -> Tuple[float, int]:
        """
        Increment a player's score.
        Returns (new_score, new_rank).
        """
        new_score = self.r.zincrby(self.key, delta, player_id)
        new_rank = self.get_rank(player_id)
        return new_score, new_rank

    def get_rank(self, player_id: str) -> Optional[int]:
        """
        Get player's rank (1-based). Returns None if player is not found.
        """
        rank_zero_based = self.r.zrevrank(self.key, player_id)
        if rank_zero_based is None:
            return None
        return rank_zero_based + 1

    def get_score(self, player_id: str) -> Optional[float]:
        """
        Get player's current score.
        """
        score = self.r.zscore(self.key, player_id)
        return float(score) if score is not None else None

    def get_top(self, n: int = 10) -> List[Dict]:
        """
        Get top N players.
        """
        results = self.r.zrevrange(self.key, 0, n - 1, withscores=True)
        return [
            {"rank": i + 1, "player": player, "score": score}
            for i, (player, score) in enumerate(results)
        ]

    def get_page(self, page: int, page_size: int = 10) -> List[Dict]:
        """
        Get a specific page of the leaderboard.
        Page is 1-based.
        """
        if page < 1:
            raise ValueError("page must be >= 1")

        start = (page - 1) * page_size
        end = start + page_size - 1
        results = self.r.zrevrange(self.key, start, end, withscores=True)
        return [
            {"rank": start + i + 1, "player": player, "score": score}
            for i, (player, score) in enumerate(results)
        ]

    def get_around_player(self, player_id: str, radius: int = 2) -> List[Dict]:
        """
        Get players around a specific player (for 'around me' views).
        Includes 'radius' players above and below the given player.
        """
        rank_zero_based = self.r.zrevrank(self.key, player_id)
        if rank_zero_based is None:
            return []

        start = max(0, rank_zero_based - radius)
        end = rank_zero_based + radius
        results = self.r.zrevrange(self.key, start, end, withscores=True)

        return [
            {"rank": start + i + 1, "player": player, "score": score}
            for i, (player, score) in enumerate(results)
        ]

    def count_players(self) -> int:
        """
        Get total number of players in the leaderboard.
        """
        return self.r.zcard(self.key)

    def remove_player(self, player_id: str) -> bool:
        """
        Remove a player from the leaderboard.
        Returns True if the player was removed, False otherwise.
        """
        removed = self.r.zrem(self.key, player_id)
        return removed > 0


def demo():
    daily_lb = Leaderboard("game", scope="daily")
    alltime_lb = Leaderboard("game", scope="alltime")

    # Clean slate
    daily_lb.r.delete(daily_lb.key)
    alltime_lb.r.delete(alltime_lb.key)

    # Add same players to both
    for lb in (daily_lb, alltime_lb):
        lb.add_score("alice", 1500)
        lb.add_score("bob", 2300)
        lb.add_score("charlie", 1800)
        lb.add_score("diana", 2100)
        lb.add_score("eve", 1950)

    # ── DAILY ─────────────────────────────────────────
    print(f"=== DAILY ({daily_lb.key}) ===")

    print("\nAdding initial scores...")

    print("\nTop 3 players:")
    for e in daily_lb.get_top(3):
        print(f" #{e['rank']} {e['player']}: {e['score']}")

    print("\nCharlie's current rank and score:")
    print(" Rank:", daily_lb.get_rank("charlie"))
    print(" Score:", daily_lb.get_score("charlie"))

    print("\nIncrementing Charlie's score by 500...")
    new_score, new_rank = daily_lb.increment_score("charlie", 500)
    print(f" New score: {new_score}, new rank: {new_rank}")

    print("\nPlayers around Charlie:")
    for e in daily_lb.get_around_player("charlie", radius=2):
        print(f" #{e['rank']} {e['player']}: {e['score']}")

    print("\nPage 1 of leaderboard (page_size=3):")
    for e in daily_lb.get_page(page=1, page_size=3):
        print(f" #{e['rank']} {e['player']}: {e['score']}")

    # ── ALL-TIME ───────────────────────────────────────
    print(f"\n=== ALL-TIME ({alltime_lb.key}) ===")

    print("\nTop 3 players:")
    for e in alltime_lb.get_top(3):
        print(f" #{e['rank']} {e['player']}: {e['score']}")

    print("\nCharlie's current rank and score:")
    print(" Rank:", alltime_lb.get_rank("charlie"))
    print(" Score:", alltime_lb.get_score("charlie"))

    print("\nIncrementing Charlie's score by 500...")
    new_score, new_rank = alltime_lb.increment_score("charlie", 500)
    print(f" New score: {new_score}, new rank: {new_rank}")

    print("\nPlayers around Charlie:")
    for e in alltime_lb.get_around_player("charlie", radius=2):
        print(f" #{e['rank']} {e['player']}: {e['score']}")

    print("\nPage 1 of leaderboard (page_size=3):")
    for e in alltime_lb.get_page(page=1, page_size=3):
        print(f" #{e['rank']} {e['player']}: {e['score']}")


if __name__ == "__main__":
    demo()