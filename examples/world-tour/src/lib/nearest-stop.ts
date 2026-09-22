export interface GeoPoint {
  lat: number;
  lng: number;
}

export interface StopWithLocation {
  id: string;
  lat: number;
  lng: number;
}

/**
 * Haversine distance in kilometres between two geographic points.
 */
function haversineKm(a: GeoPoint, b: GeoPoint): number {
  const R = 6371;
  const dLat = ((b.lat - a.lat) * Math.PI) / 180;
  const dLng = ((b.lng - a.lng) * Math.PI) / 180;
  const sinLat = Math.sin(dLat / 2);
  const sinLng = Math.sin(dLng / 2);
  const h =
    sinLat * sinLat +
    Math.cos((a.lat * Math.PI) / 180) * Math.cos((b.lat * Math.PI) / 180) * sinLng * sinLng;
  return R * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
}

/**
 * Returns the nearest stop to the given origin point,
 * or `null` if the stops array is empty.
 */
export function findNearestStop(
  origin: GeoPoint,
  stops: StopWithLocation[],
): StopWithLocation | null {
  if (stops.length === 0) return null;

  let best = stops[0]!;
  let bestDist = haversineKm(origin, best);

  for (let i = 1; i < stops.length; i++) {
    const s = stops[i]!;
    const d = haversineKm(origin, s);
    if (d < bestDist) {
      best = s;
      bestDist = d;
    }
  }

  return best;
}
