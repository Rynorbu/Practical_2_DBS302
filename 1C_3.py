import redis
from typing import List, Dict, Optional
from datetime import datetime


# ─────────────────────────────────────────────
# Leaderboard Class
# ─────────────────────────────────────────────
class Leaderboard:
    def __init__(
        self,
        name: str,
        scope: str = "alltime",
        date: Optional[str] = None,
        redis_client: Optional[redis.Redis] = None,
    ) -> None:
        self.name = name
        self.scope = scope
        self.r = redis_client or redis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            decode_responses=True,
        )

        if scope == "daily":
            day = date or datetime.utcnow().strftime("%Y-%m-%d")
            self.key = f"leaderboard:{name}:daily:{day}"
            self.r.expire(self.key, 180)
        elif scope == "alltime":
            self.key = f"leaderboard:{name}:alltime"
        else:
            raise ValueError("scope must be 'daily' or 'alltime'")

    def add_score(self, player_id: str, score: float) -> int:
        self.r.zadd(self.key, {player_id: score})
        return self.get_rank(player_id)

    def get_rank(self, player_id: str) -> Optional[int]:
        rank = self.r.zrevrank(self.key, player_id)
        return rank + 1 if rank is not None else None

    def get_score(self, player_id: str) -> Optional[float]:
        score = self.r.zscore(self.key, player_id)
        return float(score) if score is not None else None

    def get_top(self, n: int = 10) -> List[Dict]:
        results = self.r.zrevrange(self.key, 0, n - 1, withscores=True)
        return [
            {"rank": i + 1, "player": player, "score": score}
            for i, (player, score) in enumerate(results)
        ]


# ─────────────────────────────────────────────
# GeoSearch Class
# ─────────────────────────────────────────────
class GeoSearchService:
    def __init__(self, key: str, redis_client: Optional[redis.Redis] = None) -> None:
        self.key = key
        self.r = redis_client or redis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            decode_responses=True,
        )

    def add_location(self, name: str, longitude: float, latitude: float) -> None:
        self.r.geoadd(self.key, [longitude, latitude, name])

    def nearby(
        self,
        longitude: float,
        latitude: float,
        radius: float,
        unit: str = "km",
    ) -> List[Dict]:
        results = self.r.geosearch(
            self.key,
            longitude=longitude,
            latitude=latitude,
            radius=radius,
            unit=unit,
            withdist=True,
            withcoord=True,
            sort="ASC",
        )
        return self._format(results)

    def nearby_by_box(
        self,
        longitude: float,
        latitude: float,
        width: float,
        height: float,
        unit: str = "km",
    ) -> List[Dict]:
        results = self.r.geosearch(
            self.key,
            longitude=longitude,
            latitude=latitude,
            width=width,
            height=height,
            unit=unit,
            withdist=True,
            withcoord=True,
            sort="ASC",
        )
        return self._format(results)

    def _format(self, results: list) -> List[Dict]:
        formatted = []
        for item in results:
            formatted.append({
                "name":      item[0],
                "distance":  round(float(item[1]), 2),
                "longitude": round(float(item[2][0]), 4),
                "latitude":  round(float(item[2][1]), 4),
            })
        return formatted


# ─────────────────────────────────────────────
# GeoLeaderboard — Combined Service
# ─────────────────────────────────────────────
class GeoLeaderboard:
    def __init__(
        self,
        game_name: str,
        redis_client: Optional[redis.Redis] = None,
    ) -> None:
        self.r = redis_client or redis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            decode_responses=True,
        )
        self.leaderboard = Leaderboard(
            name=game_name,
            scope="alltime",
            redis_client=self.r,
        )
        self.geo = GeoSearchService(
            key=f"geo:players:{game_name}",
            redis_client=self.r,
        )

    def register_player(
        self,
        player_id: str,
        score: float,
        longitude: float,
        latitude: float,
    ) -> None:
        self.leaderboard.add_score(player_id, score)
        self.geo.add_location(player_id, longitude, latitude)

    def top_players_near(
        self,
        longitude: float,
        latitude: float,
        radius: float,
        unit: str = "km",
        top_n: int = 5,
    ) -> List[Dict]:
        """
        Find top N players near a location sorted by score (highest first).
        """
        nearby_players = self.geo.nearby(longitude, latitude, radius, unit)

        if not nearby_players:
            return []

        enriched = []
        for player in nearby_players:
            name  = player["name"]
            score = self.leaderboard.get_score(name)
            rank  = self.leaderboard.get_rank(name)

            if score is not None:
                enriched.append({
                    "player":    name,
                    "score":     score,
                    "rank":      rank,
                    "distance":  player["distance"],
                    "longitude": player["longitude"],
                    "latitude":  player["latitude"],
                    "unit":      unit,
                })

        enriched.sort(key=lambda x: x["score"], reverse=True)
        return enriched[:top_n]

    def top_players_near_by_box(
        self,
        longitude: float,
        latitude: float,
        width: float,
        height: float,
        unit: str = "km",
        top_n: int = 5,
    ) -> List[Dict]:
        """
        Find top N players near a location within a bounding box sorted by score.
        """
        nearby_players = self.geo.nearby_by_box(
            longitude, latitude, width, height, unit
        )

        if not nearby_players:
            return []

        enriched = []
        for player in nearby_players:
            name  = player["name"]
            score = self.leaderboard.get_score(name)
            rank  = self.leaderboard.get_rank(name)

            if score is not None:
                enriched.append({
                    "player":    name,
                    "score":     score,
                    "rank":      rank,
                    "distance":  player["distance"],
                    "longitude": player["longitude"],
                    "latitude":  player["latitude"],
                    "unit":      unit,
                })

        enriched.sort(key=lambda x: x["score"], reverse=True)
        return enriched[:top_n]

    def closest_players(
        self,
        longitude: float,
        latitude: float,
        radius: float,
        unit: str = "km",
        top_n: int = 3,
    ) -> List[Dict]:
        """
        Return top N closest players sorted by distance (nearest first).
        Step 1 — GeoSearch: find all players within radius
        Step 2 — Enrich with leaderboard score and rank
        Step 3 — Sort by distance ascending and return top N
        """
        # Step 1 — already sorted ASC by distance from geo.nearby()
        nearby_players = self.geo.nearby(longitude, latitude, radius, unit)

        if not nearby_players:
            return []

        # Step 2 — enrich with score and rank
        enriched = []
        for player in nearby_players:
            name  = player["name"]
            score = self.leaderboard.get_score(name)
            rank  = self.leaderboard.get_rank(name)

            if score is not None:
                enriched.append({
                    "player":    name,
                    "score":     score,
                    "rank":      rank,
                    "distance":  player["distance"],
                    "longitude": player["longitude"],
                    "latitude":  player["latitude"],
                    "unit":      unit,
                })

        # Step 3 — sort by distance ascending, return top N closest
        enriched.sort(key=lambda x: x["distance"])
        return enriched[:top_n]

    def clear(self) -> None:
        self.r.delete(self.leaderboard.key)
        self.r.delete(self.geo.key)


# ─────────────────────────────────────────────
# Demo
# ─────────────────────────────────────────────
def demo():
    geo_lb = GeoLeaderboard("thimphu_game")
    geo_lb.clear()

    print("Registering players with scores and locations...\n")
    players = [
        ("player_alice",   1500, 89.6390, 27.4728),
        ("player_bob",     2300, 89.6530, 27.4712),
        ("player_charlie", 1800, 89.6490, 27.4770),
        ("player_diana",   2100, 89.6610, 27.4650),
        ("player_eve",     1950, 89.6200, 27.4800),
        ("player_frank",   2500, 89.6420, 27.4740),
        ("player_grace",   1700, 89.6350, 27.4700),
    ]

    for player_id, score, lon, lat in players:
        geo_lb.register_player(player_id, score, lon, lat)
        print(f"  Registered {player_id:<20} score: {score}  "
              f"location: ({lat}, {lon})")

    # ── Global Leaderboard ────────────────────────────
    print("\n--- Global Leaderboard (all players) ---")
    print(f"  {'Rank':<6} {'Player':<20} {'Score'}")
    print(f"  {'-' * 36}")
    for e in geo_lb.leaderboard.get_top(10):
        print(f"  #{e['rank']:<5} {e['player']:<20} {e['score']}")

    # ── Top players by score near Norzin ──────────────
    print("\n--- Top Players Near Norzin (3km, sorted by SCORE) ---")
    print(f"  {'Rank':<6} {'Player':<20} {'Score':<10} {'Distance'}")
    print(f"  {'-' * 50}")
    top_score = geo_lb.top_players_near(
        longitude=89.6390,
        latitude=27.4728,
        radius=3,
        unit="km",
        top_n=5,
    )
    for e in top_score:
        print(f"  #{e['rank']:<5} {e['player']:<20} {e['score']:<10} "
              f"{e['distance']} {e['unit']}")

    # ── Closest players near Norzin ───────────────────
    print("\n--- Closest Players Near Norzin (3km, sorted by DISTANCE) ---")
    print(f"  {'Rank':<6} {'Player':<20} {'Score':<10} {'Distance'}")
    print(f"  {'-' * 50}")
    closest = geo_lb.closest_players(
        longitude=89.6390,
        latitude=27.4728,
        radius=3,
        unit="km",
        top_n=3,
    )
    for e in closest:
        print(f"  #{e['rank']:<5} {e['player']:<20} {e['score']:<10} "
              f"{e['distance']} {e['unit']}")

    # ── Side by side comparison ───────────────────────
    print("\n--- Comparison: Score vs Distance sorting ---")
    print(f"  {'Method':<35} {'Players (in order)'}")
    print(f"  {'-' * 70}")
    score_names    = " → ".join([e["player"].replace("player_", "") for e in top_score])
    distance_names = " → ".join([e["player"].replace("player_", "") for e in closest])
    print(f"  {'Sorted by score (highest first)':<35} {score_names}")
    print(f"  {'Sorted by distance (nearest first)':<35} {distance_names}")

    print("\n  Note:")
    print("  Score sort   → best players near you (competitive view)")
    print("  Distance sort → nearest players near you (proximity view)")


if __name__ == "__main__":
    demo()