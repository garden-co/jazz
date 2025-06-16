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

export const Root = co.map({
	people: People
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
				people: People.create([
					Person.create({ name: 'John', age: 30, dog: Dog.create({ name: 'Rex' }) }, publicGroup),
					Person.create({ name: 'Mathieu', age: 30, dog: Dog.create({ name: 'Bibi' }) }, publicGroup),
				])
			}, publicGroup);
		}
	});
