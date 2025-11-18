import { co, z } from "jazz-tools";

// #region Basic
const Person = co.map({
  firstName: z.string(),
  lastName: z.string(),
  dateOfBirth: z.date(),
});
type Person = co.loaded<typeof Person>;

export function getPersonFullName(person: Person) {
  return `${person.firstName} ${person.lastName}`;
}

function differenceInYears(date1: Date, date2: Date) {
  const diffTime = Math.abs(date1.getTime() - date2.getTime());
  return Math.ceil(diffTime / (1000 * 60 * 60 * 24 * 365.25));
}

export function getPersonAgeAsOf(person: Person, date: Date) {
  return differenceInYears(date, person.dateOfBirth);
}

const person = Person.create({
  firstName: "John",
  lastName: "Doe",
  dateOfBirth: new Date("1990-01-01"),
});

const fullName = getPersonFullName(person);
const age = getPersonAgeAsOf(person, new Date());
// #endregion

// #region UpdateHelper
export function updatePersonName(person: Person, fullName: string) {
  const [firstName, lastName] = fullName.split(" ");
  person.$jazz.set("firstName", firstName);
  person.$jazz.set("lastName", lastName);
}

console.log(person.firstName, person.lastName); // John Doe

updatePersonName(person, "Jane Doe");

console.log(person.firstName, person.lastName); // Jane Doe
// #endregion
