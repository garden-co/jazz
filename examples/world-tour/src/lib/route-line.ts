interface Stop {
  date: Date;
  lng: number;
  lat: number;
}

interface LineString {
  type: "LineString";
  coordinates: [number, number][];
}

export function computeRouteLine(stops: Stop[]): LineString | null {
  if (stops.length < 2) return null;

  const sorted = [...stops].sort((a, b) => a.date.getTime() - b.date.getTime());

  return {
    type: "LineString",
    coordinates: sorted.map((s) => [s.lng, s.lat]),
  };
}
