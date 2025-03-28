---

**Jazz Schema Rules:**

1. **User Profile and Account**
   1.1. Define `export class UserProfile extends Profile` with exactly one property:
   ```ts
   name = co.string;
   ```
   1.2. Add a static `validate` method in `UserProfile` that checks `name` is present and non-empty.
   1.3. Define `export class JazzAccount extends Account` with exactly two properties:
   ```ts
   profile = co.ref(UserProfile);
   root = co.ref(AccountRoot);
   ```
   1.4. The `JazzAccount` class must have a `migrate(creationProps?: { name: string; other?: Record<string, unknown> })` method.
      - Within `migrate`, if `this._refs.root` is undefined and `creationProps` is provided, run `initialMigration`.
      - The `creationProps` **must** include a `name` property; `other` is optional but do not define more fields.

2. **Container, Root & Ownership**
   2.1. The `AccountRoot` class (extending `CoMap`) **must** have a `container` property referencing a `Container`.
   2.2. The `Container` class (extending `CoMap`) should contain the main domain entities of the app.
   2.3. **Never** define a `name` field in the `Container` class. The template shows an example `name` property for a Container, but these rules override that.
   2.4. Whenever the root structure is initialized, it is always owned by the current `JazzAccount`:
   ```ts
   this.root = AccountRoot.create({ container: defaultContainer, version: 0 }, { owner: this });
   ```

3. **Groups & Ownership**
   3.1. If the `UserProfile` is intended to be public, set its owner to a `publicGroup` that has `"everyone"` as `"reader"`. Otherwise, use a private group.
   3.2. When creating a group, no need to explicitly pass `owner: this`. That is implicit if it's the same account.
   3.3. **Do not** use properties like `user`, `users`, `group`, or `groups` in CoMaps or CoLists. Ownership is implicit.

4. **No Direct CoList Fields**
   4.1. **Never** do:
   ```ts
   co.ref(CoList.Of(co.ref(SomeClass)));
   ```
   4.2. Instead, define a CoList class (e.g. `export class SomeClassList extends CoList.Of(co.ref(SomeClass)) {}`) and reference it.

5. **Schema Structure & Fields**
   5.1. Follow the provided template patterns. **Do not** add extra entities or fields outside the user's requirements or the template.
   5.2. Do **not** use properties like `createdAt` or `updatedAt`; they're implicit in CoValue.
   5.3. If a property is optional, denote it with a question mark (`?`) in the field definition, or use `co.optional.*`.
   5.4. Keep comments from the template, especially around migration blocks, intact.
   5.5. Never set a property to "co.ref(UserProfile)".

6. **Output & Formatting**
   6.1. Generate the final schema in TypeScript with no extra markdown or triple backticks.
   6.2. Do **not** expand or alter the template's classes beyond what is required.
   6.3. Avoid redundant or conflicting rules from the template; these revised rules take priority.

7. **Loading & Resolving Data (0.12.0+)**
   7.1. **Always** use the new resolve API pattern when loading data:
   ```ts
   // Use this pattern (0.12.0+)
   const data = await MyClass.load(id, { resolve: true });

   // NOT this pattern (pre-0.12.0)
   const data = await MyClass.load(id, []);
   ```
   7.2. For shallow loading of collections, use `true` instead of empty arrays or objects:
   ```ts
   // Correct (0.12.0+)
   await ListClass.load(id, { resolve: true });

   // Incorrect (pre-0.12.0)
   await ListClass.load(id, []);
   ```
   7.3. For deep loading of collections, use the `$each` key:
   ```ts
   // For CoList
   await TaskList.load(id, {
     resolve: { $each: true }
   });

   // For nested structures
   await Project.load(id, {
     resolve: {
       tasks: { $each: true }
     }
   });
   ```
   7.4. Use the `Resolved` type helper for type-safe access to loaded data:
   ```ts
   type TaskListResolved = Resolved<TaskList, {
     $each: { assignee: true }
   }>;

   function Component({ tasks }: { tasks: TaskListResolved }) {
     // Type-safe access
     return tasks.map(task => task.assignee);
   }
   ```

---

Continue: 4_1_jazz_example.md
