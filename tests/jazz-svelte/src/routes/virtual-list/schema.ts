import { co, Group, z } from "jazz-tools";

export const Dog = co.map({
  name: z.string(),
});

export const Person = co.map({
  name: z.string(),
  age: z.number(),
  dog: Dog,
});

export const People = co.list(Person);

export const PeopleRecord = co.record(z.string(), People);
export const Root = co.map({
  people: PeopleRecord,
});

export const TestAccount = co
  .account({
    profile: co.profile(),
    root: Root,
  })
  .withMigration((account) => {
    if (!account.$jazz.refs.root) {
      const publicGroup = Group.create();
      publicGroup.makePublic("writer");

      account.$jazz.set(
        "root",
        Root.create(
          {
            people: {
              alice: Array.from({ length: 50 }, (_, i) => ({
                name: `Alice ${i}`,
                age: 30,
                dog: { name: `Dog ${i}` },
              })),
              bob: Array.from({ length: 50 }, (_, i) => ({
                name: `Bob ${i}`,
                age: 30,
                dog: { name: `Dog ${i}` },
              })),
              john: Array.from({ length: 50 }, (_, i) => ({
                name: `John ${i}`,
                age: 30,
                dog: { name: `Dog ${i}` },
              })),
            },
          },
          publicGroup,
        ),
      );
    }
  });
