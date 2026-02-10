// #region Task
export const Task = co.map({
  title: z.string(),
  completed: z.boolean(),
});
// #endregion

// #region Lists
export const ListOfColors = co.list(z.string());
export const ListOfTasks = co.list(Task);
// #endregion

// #region Basic
import { co, z } from "jazz-tools";

export const TodoProject = co.map({
  title: z.string(),
  tasks: ListOfTasks,
});
// #endregion

export const Fruit = co.map({
  name: z.string(),
  color: z.string(),
});
// #region Records
export const ColourToHex = co.record(z.string(), z.string());
export const ColorToFruit = co.record(z.string(), Fruit);
// #endregion

// #region Feed
export const FeedOfTasks = co.feed(Task);
// #endregion

// #region FileStream
export const Document = co.map({
  title: z.string(),
  file: co.fileStream(),
});
// #endregion

// #region PrimitiveTypes
z.string(); // For simple strings
z.number(); // For numbers
z.boolean(); // For booleans
z.date(); // For dates
z.literal(["waiting", "ready"]); // For enums
// #endregion

// #region ZObject
const Sprite = co.map({
  // assigned as a whole
  position: z.object({ x: z.number(), y: z.number() }),
});
// #endregion

// #region ZTuple
const SpriteWithTuple = co.map({
  // assigned as a whole
  position: z.tuple([z.number(), z.number()]),
});
// #endregion

// #region References
const Person = co.map({
  name: z.string(),
});

const ListOfPeople = co.list(Person);

const Company = co.map({
  members: ListOfPeople,
});
// #endregion

const Pet = co.map({
  name: z.string(),
});
// #region OptionalProperties
const PersonWithOptionalProperties = co.map({
  age: z.optional(z.number()), // primitive
  pet: co.optional(Pet), // CoValue
});
// #endregion

// #region SelfReferencing
const SelfReferencingPerson = co.map({
  name: z.string(),
  get bestFriend() {
    return SelfReferencingPerson;
  },
});
// #endregion

// #region MutuallyRecursive
const MutuallyRecursivePerson = co.map({
  name: z.string(),
  get friends() {
    return ListOfFriends;
  },
});

const ListOfFriends = co.list(MutuallyRecursivePerson);
// #endregion
