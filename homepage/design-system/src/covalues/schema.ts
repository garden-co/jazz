import { CoFeed, CoList, CoMap, coField } from "jazz-tools";

// Example CoMap class
export class Person extends CoMap {
  name = coField.string;
  age = coField.number;
  height = coField.optional.number;
  weight = coField.optional.number;
}

export class ListOfPeople extends CoList.Of(coField.ref(Person)) {}

export class PersonFeed extends CoFeed.Of(coField.ref(Person)) {}
