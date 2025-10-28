import { co, z } from "jazz-tools";

// Example CoMap class
export const Person = co.map({
  name: z.string(),
  age: z.number(),
  height: z.number().optional(),
  weight: z.number().optional(),
});

export const ListOfPeople = co.list(Person);

export const PersonFeed = co.feed(Person);
