import redis
from typing import List, Dict, Optional


class GeoSearchService:
    """
    Geo-search functionality using Redis geospatial indexes.
    Supports radius search and bounding box search.
    """

    def __init__(self, key: str, redis_client: Optional[redis.Redis] = None) -> None:
        self.key = key
        self.r = redis_client or redis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            decode_responses=True,
        )

    def add_location(self, name: str, longitude: float, latitude: float) -> None:
        """
        Add or update a named location.
        """
        self.r.geoadd(self.key, [longitude, latitude, name])

    def distance_between(
        self,
        name1: str,
        name2: str,
        unit: str = "km",
    ) -> Optional[float]:
        """
        Compute distance between two locations.
        Units: 'm', 'km', 'mi', 'ft'
        """
        dist = self.r.geodist(self.key, name1, name2, unit)
        return round(float(dist), 2) if dist is not None else None

    def nearby(
        self,
        longitude: float,
        latitude: float,
        radius: float,
        unit: str = "km",
    ) -> List[Dict]:
        """
        Find locations within a given radius from a coordinate point.
        Uses GEOSEARCH with FROMLONLAT BYRADIUS.
        """
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

    def nearby_from_member(
        self,
        member: str,
        radius: float,
        unit: str = "km",
    ) -> List[Dict]:
        """
        Find locations within a given radius from an existing member.
        Uses GEOSEARCH with FROMMEMBER BYRADIUS.
        """
        results = self.r.geosearch(
            self.key,
            member=member,
            radius=radius,
            unit=unit,
            withdist=True,
            withcoord=True,
            sort="ASC",
        )
        return self._format(results)

    # ── Exercise 1 — BYBOX ────────────────────────────
    def nearby_by_box(
        self,
        longitude: float,
        latitude: float,
        width: float,
        height: float,
        unit: str = "km",
    ) -> List[Dict]:
        """
        Find locations within a bounding box centered at a coordinate point.
        Uses GEOSEARCH with FROMLONLAT BYBOX width height.

        width  = east-west span
        height = north-south span
        """
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

    def nearby_by_box_from_member(
        self,
        member: str,
        width: float,
        height: float,
        unit: str = "km",
    ) -> List[Dict]:
        """
        Find locations within a bounding box centered at an existing member.
        Uses GEOSEARCH with FROMMEMBER BYBOX width height.
        """
        results = self.r.geosearch(
            self.key,
            member=member,
            width=width,
            height=height,
            unit=unit,
            withdist=True,
            withcoord=True,
            sort="ASC",
        )
        return self._format(results)

    def _format(self, results: list) -> List[Dict]:
        """
        Format raw redis geosearch results into clean dicts.
        Each result is (name, dist, (lon, lat)).
        """
        formatted = []
        for item in results:
            name      = item[0]
            distance  = round(float(item[1]), 2)
            longitude = round(float(item[2][0]), 4)
            latitude  = round(float(item[2][1]), 4)
            formatted.append({
                "name":      name,
                "distance":  distance,
                "longitude": longitude,
                "latitude":  latitude,
            })
        return formatted


def demo():
    service = GeoSearchService("geo:stores:thimphu")

    # Clean slate
    service.r.delete(service.key)

    # Add stores in Thimphu
    print("Adding store locations...")
    service.add_location("store_norzin",      89.6390, 27.4728)
    service.add_location("store_changzamtog", 89.6530, 27.4712)
    service.add_location("store_motithang",   89.6490, 27.4770)
    service.add_location("store_babesa",      89.6610, 27.4650)
    service.add_location("store_paro_rd",     89.6200, 27.4800)

    # ── Distance ──────────────────────────────────────
    print("\n--- Distance ---")
    dist = service.distance_between("store_norzin", "store_changzamtog")
    print(f"  Norzin -> Changzamtog : {dist} km")

    dist2 = service.distance_between("store_norzin", "store_motithang")
    print(f"  Norzin -> Motithang   : {dist2} km")

    # ── Nearby by Radius ──────────────────────────────
    print("\n--- Nearby (radius 3 km from Norzin coordinates) ---")
    radius_results = service.nearby(89.6390, 27.4728, radius=3, unit="km")
    for s in radius_results:
        print(f"  {s['name']:<25} dist: {s['distance']} km  "
              f"coords: ({s['latitude']}, {s['longitude']})")

    # ── Nearby from Member ────────────────────────────
    print("\n--- Nearby (radius 3 km from store_norzin member) ---")
    member_results = service.nearby_from_member("store_norzin", radius=3, unit="km")
    for s in member_results:
        print(f"  {s['name']:<25} dist: {s['distance']} km  "
              f"coords: ({s['latitude']}, {s['longitude']})")

    # ── Exercise 1: BYBOX from coordinates ───────────
    print("\n--- Exercise 1: BYBOX (5km x 5km box from Norzin coordinates) ---")
    box_results = service.nearby_by_box(
        longitude=89.6390,
        latitude=27.4728,
        width=5,
        height=5,
        unit="km",
    )
    for s in box_results:
        print(f"  {s['name']:<25} dist: {s['distance']} km  "
              f"coords: ({s['latitude']}, {s['longitude']})")

    # ── Exercise 1: BYBOX from member ─────────────────
    print("\n--- Exercise 1: BYBOX (5km x 5km box from store_norzin member) ---")
    box_member_results = service.nearby_by_box_from_member(
        member="store_norzin",
        width=5,
        height=5,
        unit="km",
    )
    for s in box_member_results:
        print(f"  {s['name']:<25} dist: {s['distance']} km  "
              f"coords: ({s['latitude']}, {s['longitude']})")

    # ── Comparison: radius vs box ─────────────────────
    print("\n--- Comparison: Radius vs Box ---")
    print(f"  Stores found in 3km radius : {len(radius_results)}")
    print(f"  Stores found in 5x5km box  : {len(box_results)}")
    print("\n  Note: A radius search is circular.")
    print("  A box search is rectangular — catches corners a circle would miss.")


if __name__ == "__main__":
    demo()