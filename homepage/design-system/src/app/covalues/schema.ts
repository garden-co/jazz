import { CoFeed, CoList, CoMap, co } from "jazz-tools";

// Example CoMap class
export class Person extends CoMap {
  name = co.string;
  age = co.number;
  height = co.optional.number;
  weight = co.optional.number;
}

export class ListOfPeople extends CoList.Of(co.ref(Person)) {}

export class PersonFeed extends CoFeed.Of(co.ref(Person)) {}
