import { schema as s } from "jazz-tools";

const schema = {
  tours: s.table({
    name: s.string(),
    band_id: s.string(),
    starts_at: s.timestamp(),
    ends_at: s.timestamp(),
  }),
  venues: s.table({
    name: s.string(),
    city: s.string(),
    country: s.string(),
    latitude: s.float(),
    longitude: s.float(),
    time_zone: s.string(),
  }),
  members: s.table({
    tour_id: s.ref("tours"),
    user_id: s.string(),
    role: s.enum("manager", "crew", "artist"),
  }),
  legs: s.table({
    tour_id: s.ref("tours"),
    venue_id: s.ref("venues"),
    starts_at: s.timestamp(),
    ends_at: s.timestamp(),
    status: s.enum("draft", "confirmed", "cancelled"),
    notes: s.string().optional(),
  }),
  events: s.table({
    leg_id: s.ref("legs"),
    kind: s.enum("load_in", "soundcheck", "show", "travel"),
    starts_at: s.timestamp(),
    ends_at: s.timestamp(),
  }),
  travel_days: s.table({
    tour_id: s.ref("tours"),
    from_leg_id: s.ref("legs"),
    to_leg_id: s.ref("legs"),
    starts_at: s.timestamp(),
    ends_at: s.timestamp(),
    mode: s.string(),
  }),
};
type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
