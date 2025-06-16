import { co, Group, z } from 'jazz-tools';

export const Dog = co.map({
	name: z.string()
});

export const Person = co.map({
	name: z.string(),
	age: z.number(),
	dog: Dog
});

export const People = co.list(Person);

export const PeopleRecord = co.record(z.string(), People);
export const Root = co.map({
	people: PeopleRecord
});

export const TestAccount = co
	.account({
		profile: co.profile(),
		root: Root
	})
	.withMigration(account => {
		if (!account._refs.root) {
			const publicGroup = Group.create();
			publicGroup.makePublic("writer");

			account.root = Root.create({
				people: PeopleRecord.create({
					alice: People.create(Array.from({ length: 50 }, (_, i) => Person.create({ name: `Alice ${i}`, age: 30, dog: Dog.create({ name: `Dog ${i}` }) }, publicGroup))),
					bob: People.create(Array.from({ length: 50 }, (_, i) => Person.create({ name: `Bob ${i}`, age: 30, dog: Dog.create({ name: `Dog ${i}` }) }, publicGroup))),
					john: People.create(Array.from({ length: 50 }, (_, i) => Person.create({ name: `John ${i}`, age: 30, dog: Dog.create({ name: `Dog ${i}` }) }, publicGroup))),
				})
			}, publicGroup);
		}
	});
