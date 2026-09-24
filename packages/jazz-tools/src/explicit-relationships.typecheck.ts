import { schema as s } from "./index.js";

// @ts-expect-error Required explicit relationship map, even for empty tables.
s.table({ name: s.string() });
// @ts-expect-error Only UUID columns may back relationships.
s.table({ title: s.string() }, { author: s.rel("users", "title") });
// @ts-expect-error The local column must exist.
s.table({ authorId: s.uuid() }, { author: s.rel("users", "missing") });
// @ts-expect-error Relations cannot shadow their own columns.
s.table({ author: s.uuid() }, { author: s.rel("users", "author") });
// @ts-expect-error Row ID is reserved.
s.table({ authorId: s.uuid() }, { id: s.rel("users", "authorId") });
s.table(
  { authorId: s.uuid() },
  {
    // @ts-expect-error One reference column cannot target two tables.
    author: s.rel("users", "authorId"),
    // @ts-expect-error Conflicting target on the same column.
    writer: s.rel("writers", "authorId"),
  },
);
const invalidTarget = {
  posts: s.table({ authorId: s.uuid() }, { author: s.rel("missing", "authorId") }),
};
// @ts-expect-error Unknown table rejected at schema construction.
s.defineSchema(invalidTarget);
// @ts-expect-error Unknown table rejected at app construction.
s.defineApp(invalidTarget);
// @ts-expect-error Whole schema validated even before slicing.
s.defineSliceableApp(invalidTarget);
const invalidReverse = {
  posts: s.table({ authorId: s.uuid() }, { author: s.rel("users", "authorId") }),
  users: s.table({}, { posts: s.reverse("posts", "authorId") }),
};
// @ts-expect-error Reverse references a forward name, not its column.
s.defineSchema(invalidReverse);
const wrongTarget = {
  posts: s.table({ authorId: s.uuid() }, { author: s.rel("users", "authorId") }),
  users: s.table({}, {}),
  others: s.table({}, { posts: s.reverse("posts", "author") }),
};
// @ts-expect-error Reverse must point back to its declaring table.
s.defineSchema(wrongTarget);
const schema = s.defineSchema({
  posts: s
    .table({ authorId: s.uuid() }, { author: s.rel("users", "authorId") })
    .indexOnly(["authorId"]),
  users: s.table({ name: s.string() }, { authored: s.reverse("posts", "author") }).branchBy("name"),
});
const app = s.defineApp(schema);
app.posts.include({ author: true });
app.users.hopTo("authored");
// @ts-expect-error No inferred inverse.
app.users.hopTo("postsViaAuthor");
// @ts-expect-error Stored UUID column is not a relationship alias.
app.posts.hopTo("authorId");
const slice = s.defineSliceableApp(schema).slice("posts", "users");
slice.posts.include({ author: true });
slice.users.hopTo("authored");
// @ts-expect-error Legacy shortcut removed.
s.ref("users");

// @ts-expect-error Provenance names cannot be relationship aliases.
s.table({ authorId: s.uuid() }, { $createdAt: s.rel("users", "authorId") });
// @ts-expect-error Prototype aliases cannot be represented safely in result objects.
s.table({ authorId: s.uuid() }, { ["__proto__"]: s.rel("users", "authorId") });

const migrationFrom = {
  posts: s.table({ owner: s.uuid() }, { author: s.rel("users", "owner") }),
  users: s.table({}, {}),
  teams: s.table({}, {}),
};
const migrationTo = {
  ...migrationFrom,
  posts: s.table({ owner: s.uuid() }, { author: s.rel("teams", "owner") }),
};
// @ts-expect-error Changing the effective stored reference target requires a supported migration, not an empty lens.
s.defineMigration({ from: migrationFrom, to: migrationTo });
s.defineMigration({
  from: migrationFrom,
  to: {
    ...migrationFrom,
    posts: s.table({ owner: s.uuid() }, { writer: s.rel("users", "owner") }),
  },
});

// Permission helpers use the same declared names in either direction.
s.definePermissions(app, ({ policy, allowedTo }) => {
  policy.posts.allowRead.where(allowedTo.read("author"));
  policy.users.allowRead.where(allowedTo.read("authored"));
  policy.users.allowDelete.where(allowedTo.deleteReferencing(policy.posts, "author"));
  // @ts-expect-error Stored UUID column is not a relationship name.
  allowedTo.update("authorId");
  // @ts-expect-error No convention-derived reverse name.
  allowedTo.read("postsViaAuthor");
  // @ts-expect-error Names must be declared somewhere in the app.
  allowedTo.insert("missing");
});
