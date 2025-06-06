import { co, z } from "jazz-tools";

export const Dog = co.map({
    name: z.string(),
});

export const Person = co.map({
    name: z.string(),
    age: z.number(),
    dog: Dog,
});

export const MyAccount = co.account({
    profile: co.profile(),
    root: Person,
}).withMigration((account) => {
    if (!account._refs.root) {
        account.root = Person.create({ name: "John", age: 30, dog: Dog.create({ name: "Rex" }, account) }, account);
    }
});
