export interface SeedVenue {
  name: string;
  city: string;
  country: string;
  lat: number;
  lng: number;
  capacity?: number;
}

export interface SeedStop {
  venueIndex: number; // index into venues array
  date: string; // ISO date string
  status: "confirmed" | "tentative" | "cancelled";
  publicDescription: string;
  privateNotes?: string;
}

export const bandName = "The Placeholder Band";

export const venues: SeedVenue[] = [
  { name: "O2 Arena", city: "London", country: "UK", lat: 51.503, lng: 0.003, capacity: 20000 },
  {
    name: "Madison Square Garden",
    city: "New York",
    country: "USA",
    lat: 40.7505,
    lng: -73.9934,
    capacity: 20789,
  },
  { name: "Budokan", city: "Tokyo", country: "Japan", lat: 35.6932, lng: 139.75, capacity: 14471 },
  {
    name: "Olympiastadion",
    city: "Berlin",
    country: "Germany",
    lat: 52.5147,
    lng: 13.2395,
    capacity: 74475,
  },
  {
    name: "Allianz Parque",
    city: "São Paulo",
    country: "Brazil",
    lat: -23.5275,
    lng: -46.6781,
    capacity: 43713,
  },
  {
    name: "Qudos Bank Arena",
    city: "Sydney",
    country: "Australia",
    lat: -33.8468,
    lng: 151.0694,
    capacity: 21032,
  },
  {
    name: "AccorHotels Arena",
    city: "Paris",
    country: "France",
    lat: 48.8386,
    lng: 2.3786,
    capacity: 20300,
  },
  {
    name: "Movistar Arena",
    city: "Buenos Aires",
    country: "Argentina",
    lat: -34.6345,
    lng: -58.4122,
    capacity: 15500,
  },
  {
    name: "Rogers Centre",
    city: "Toronto",
    country: "Canada",
    lat: 43.6414,
    lng: -79.3894,
    capacity: 49282,
  },
  {
    name: "Cape Town Stadium",
    city: "Cape Town",
    country: "South Africa",
    lat: -33.9035,
    lng: 18.4113,
    capacity: 64100,
  },
];

export const stops: SeedStop[] = [
  {
    venueIndex: 0,
    date: "2026-06-15",
    status: "confirmed",
    publicDescription: "Opening night of the World Tour!",
    privateNotes: "Sound check at 2pm. Rider: no brown M&Ms.",
  },
  {
    venueIndex: 6,
    date: "2026-06-22",
    status: "confirmed",
    publicDescription: "Paris summer show",
    privateNotes: "French promoter contact: Jean-Pierre",
  },
  {
    venueIndex: 3,
    date: "2026-06-29",
    status: "tentative",
    publicDescription: "Berlin stadium show",
    privateNotes: "Waiting on permit approval",
  },
  {
    venueIndex: 1,
    date: "2026-07-10",
    status: "confirmed",
    publicDescription: "NYC! The Garden!",
    privateNotes: "After-party at rooftop bar TBD",
  },
  {
    venueIndex: 8,
    date: "2026-07-17",
    status: "confirmed",
    publicDescription: "Toronto summer festival tie-in",
  },
  {
    venueIndex: 4,
    date: "2026-07-28",
    status: "tentative",
    publicDescription: "São Paulo mega-show",
    privateNotes: "Visa paperwork in progress",
  },
  {
    venueIndex: 7,
    date: "2026-08-05",
    status: "cancelled",
    publicDescription: "Buenos Aires (cancelled)",
    privateNotes: "Venue double-booked, looking for alternatives",
  },
  {
    venueIndex: 9,
    date: "2026-08-15",
    status: "confirmed",
    publicDescription: "Cape Town — first time in Africa!",
  },
  {
    venueIndex: 2,
    date: "2026-09-01",
    status: "confirmed",
    publicDescription: "Tokyo finale!",
    privateNotes: "Extended set — 3 hours. Pyrotechnics approved.",
  },
  {
    venueIndex: 5,
    date: "2026-09-10",
    status: "tentative",
    publicDescription: "Sydney encore — if demand warrants",
    privateNotes: "Ticket pre-sale numbers due by July",
  },
];
