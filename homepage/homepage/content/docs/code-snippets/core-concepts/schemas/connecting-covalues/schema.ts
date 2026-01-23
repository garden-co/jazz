import { co, z, Loaded, Group, Account } from "jazz-tools";

export const Location = co.map({
  city: z.string(),
  country: z.string(),
});
export type Location = co.loaded<typeof Location>;

const Actor = co.map({
  name: z.string,
  imageURL: z.string,
  birthplace: Location, // Links directly to the Location CoMap above.
});
export type Actor = co.loaded<typeof Actor>;

//  actual actor data is stored in the separate Actor CoValue
const Movie = co.map({
  title: z.string,
  director: z.string,
  cast: co.list(Actor), // ordered, mutable
});
export type Movie = co.loaded<typeof Movie>;

// A User CoMap can maintain a CoFeed of Movie to track their favorite movies
const User = co.map({
  username: z.string,
  favoriteMovies: co.feed(Movie), // append-only
});
export type User = co.loaded<typeof User>;
