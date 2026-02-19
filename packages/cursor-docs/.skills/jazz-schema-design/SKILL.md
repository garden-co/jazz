---
name: jazz-schema-design
description: Design and implement collaborative data schemas using the Jazz framework. Use this skill when building or working with Jazz apps to define data structures using CoValues. This skill focuses exclusively on schema definition and data modeling logic.
---
# Jazz Data Modelling and Schema Design

## When to Use This Skill

* Designing data structures for Jazz applications
* Defining CoValue schemas and relationships
* Configuring permissions at the schema level
* Planning schema evolution and migrations
* Choosing between scalar and collaborative types
* Modeling relationships between data entities

## Do NOT Use This Skill For

* Writing tests for Jazz applications (use the `jazz-testing` skill)
* General framework integration questions (use the `jazz-ui-development` skill)
* Permissions topics outside the schema. Use the `jazz-permissions-security` skill.

**Key Heuristic for Agents:** If the user is asking about how to structure their data model, define relationships, configure default permissions, or evolve schemas, use this skill.

## Core Concepts

Jazz models data as an explicitly linked collaborative graph, not traditional tables or collections. Data types are defined as schemas, with CoValues serving as the fundamental building blocks.

## Schema Definition Basics

### Basic Structure

```ts
const Author = co.map({
  name: z.string()
});

const Post = co.map({
  title: z.string(),
  content: co.richText(),
});
```

**Key Libraries:**

- `z.*` - Zod schemas for primitive types (e.g., `z.string()`)
- `co.*` - Jazz collaborative data types (e.g., `co.richText()`, `co.map()`)

## Permissions Model

Permissions are integral to the data model, not an afterthought. Each CoValue has an ownership group with hierarchical permissions.

### Permission Levels (cumulative)

1. **none** - Cannot read content
2. **reader** - Can read content
3. **writer** - Can update content (overwrite values, modify lists)
4. **admin** - Can grant/revoke permissions plus all writer capabilities

### Critical Permission Rules

- **Only the creator is admin by default** - no superuser concept
- **Cannot change CoValue ownership group** - must modify group membership instead
- **Different permissions require separate containers** - use distinct CoMaps/CoLists
- **Nested CoValues inherit permissions** from parent by default
- Define default permissions at schema level during design

### Defining Permissions at Schema Level

Use `withPermissions()` to set automatic permissions when creating CoValues:

```ts
const Dog = co.map({
  name: z.string(),
}).withPermissions({
  onInlineCreate: "sameAsContainer",
});

const Person = co.map({
  pet: Dog,
}).withPermissions({
  default: () => Group.create().makePublic(),
});

// Person CoValues are public, Dog shares owner with Person
const person = Person.create({
  pet: { name: "Rex" }
});
```

#### Permission Configuration Options

**`default`**
Defines group when calling `.create()` without explicit owner.

**`onInlineCreate`**
Controls behavior when CoValue is created inline (NOT applied to `.create()` calls):

- `"extendsContainer"` (default) - New group includes container owner as member, inheriting permissions
- `"sameAsContainer"` - Reuse container's owner (performance optimization—see below for concerns and considerations)
- `"newGroup"` - New group with active account as admin
- `{ extendsContainer: "reader" }` - Like `"extendsContainer"` but override container owner's role
- Custom callback - Create and configure new group as needed

**`onCreate`**
Callback runs on every CoValue creation (both `.create()` and inline). Use to configure owner.

#### Global Permission Defaults

Set defaults for all schemas using `setDefaultSchemaPermissions`:

```ts
import { setDefaultSchemaPermissions } from "jazz-tools";

setDefaultSchemaPermissions({
  onInlineCreate: "sameAsContainer",  // Performance optimization
});
```

**USE EXTREME CAUTION:** If you use `sameAsContainer`, you **MUST** be aware that the child and parent groups are one and the same. Any changes to the child group will affect the parent group, and vice versa. This can lead to unexpected behavior if not handled carefully, where changing permissions on a child group inadvertently results in permissions being granted to the parent group and any other siblings created with the same parent. As ownership cannot be changed, you **MUST NOT USE** `sameAsContainer` if you **AT ANY TIME IN FUTURE** may wish to change permissions granularly on the child group.

## CoValue Types

| TypeScript Type | CoValue | Use Case |
|----------------|---------|----------|
| `object` | **CoMap** | Struct-like objects with predefined keys |
| `Record<string, T>` | **CoRecord** | Dict-like objects with arbitrary string keys |
| `T[]` | **CoList** | Ordered lists |
| `T[]` (append-only) | **CoFeed** | Session-based append-only lists |
| `string` | **CoPlainText/CoRichText** | Collaborative text editing |
| `Blob \| File` | **FileStream** | File storage |
| `Blob \| File` (image) | **ImageDefinition** | Image storage |
| `number[] \| Float32Array` | **CoVector** | Embeddings/vector data |
| `T \| U` (discriminated) | **DiscriminatedUnion** | Mixed-type lists |

Use the special types `co.account()` and `co.profile()` for user accounts and profiles.

## Choosing Scalar vs Collaborative Types

### Scalar Types (Zod: `z.*`)

**Use when:**

- Full replacement updates expected
- No collaborative editing needed
- Single writer scenario
- Raw performance critical

**Examples:**

```ts
const myCoValue = co.map({
  title: z.string()        // Replace entire title, no collaboration needed
  coords: z.object({
    lat: z.number(),
    lon: z.number()
  })   // Replace entire object, no collaboration needed
});
```

Note: not all Zod types are available in Jazz. Be sure to **always** `import { z } from 'jazz-tools';`, and validate whether the type exists on the export. **DO NOT** import from `zod`.

### Collaborative Types (Jazz: `co.*`)

**Use when:**

- Multiple users edit simultaneously
- Surgical/granular edits needed
- Full edit history tracking valuable
- Collaborative features required

**Examples:**

```ts
const myCoVal = co.map({
  content: co.richText()   // Multiple editors
  items: co.list(Item)     // Add/remove individual items
  config: co.map({
    settingA: z.boolean(),
    settingB: z.number()
  })       // Update specific keys
});
```

**Trade-off:** CoValues track full edit history. *Slightly* slower for single-writer full-replacement scenarios, but benefits almost always outweigh costs.

## Relationship Modeling

### One-Directional Reference

```ts
const Post = co.map({
  title: z.string(),
  author: Author  // One-way reference (like foreign key)
});
```

Jazz stores referenced ID. Use resolve queries to control reference traversal depth.

### Recursive/Forward References

Use getters to defer schema evaluation:

```ts
const Author = co.map({
  name: z.string(),
  get posts() {
    return co.list(Post);  // Deferred evaluation
  }
});

const Post = co.map({
  title: z.string(),
  author: Author
});
```

**Important:** Jazz doesn't create inferred inverse relationships. Explicitly add both sides for bidirectional traversal.

### Inverse Relationships (Two-Way)

**One-to-One:**

```ts
const Author = co.map({
  name: z.string(),
  get post() {
    return Post;
  }
});

const Post = co.map({
  title: z.string(),
  author: Author
});
```

**One-to-Many:**

```ts
const Author = co.map({
  name: z.string(),
  get posts() {
    return co.list(Post);
  }
});

const Post = co.map({
  author: Author
});
```

**Many-to-Many:**

Use `co.list()` at both ends. Jazz doesn't maintain consistency - manage in application code.

### Set-Like Collections (Unique Constraint)

CoLists allow duplicates. For uniqueness, use CoRecord keyed on ID:

```ts
const Author = co.map({
  name: z.string(),
  posts: co.record(z.string(), Post)
});

// Usage
author.posts.$jazz.set(newPost.$jazz.id, newPost);
```

Note: CoRecords always use string keys. Validate IDs at application level.

## Data Discovery Pattern

CoValues are only addressable by unique ID. Discovery without ID requires reference traversal.

**Standard pattern:**

- Attach 'root' CoValue to user account (entry point to the data graph)
- For global 'roots': hardcode ID or use environment variable
- Build graph from root via references

## Schema Evolution

Each CoValue copy is authoritative. Users may be on different schema versions simultaneously.

### Best Practices

1. **Add version field** to schema
2. **Only add fields, never remove**
3. **Never change existing field types**
4. **Make new fields optional** (backward compatible)
5. **Use `withMigration()` carefully** - runs on every load

### Migration Example

```ts
const Post = co.map({
  version: z.number().optional(),
  title: z.string(),
  content: co.richText(),
  tags: z.array(z.string()).optional()  // New optional field
}).withMigration((post) => {
  // Exit early if already migrated
  if (post.version === 2) return;
  
  // Perform migration
  if (!post.$jazz.has('tags')) {
    post.$jazz.set('tags', []);
  }
  post.$jazz.set('version', 2);
});
```

**Migration warnings:**

- Runs every time CoValue loads
- Exit early to avoid unnecessary work
- Poor migrations can significantly slow app

## Design Checklist

- [ ] Identify which data needs collaborative editing
- [ ] Map permissions requirements to CoValue containers
- [ ] Choose scalar vs collaborative types appropriately
- [ ] Define explicit relationships (both directions if needed)
- [ ] Plan root CoValue attachment strategy
- [ ] Add version field for future evolution
- [ ] Set default permissions at schema level
- [ ] Handle recursive references with getters
- [ ] Consider migration strategy for schema changes
- [ ] Ensure an initial migration exists for the user account to ensure the profile and root are initialized
- [ ] Ensure there are no TS or linting errors

## Common Patterns

### Blog with Authors and Posts

```ts
const Author = co.map({
  name: z.string(),
  bio: co.richText(),
  get posts() {
    return co.list(Post);
  }
});

const Post = co.map({
  title: z.string(),
  content: co.richText(),
  author: Author,
  publishedAt: z.date().optional()
});
```

### Collaborative Task List

```ts
const Task = co.map({
  title: z.string(),
  description: co.richText(),
  completed: z.boolean(),
  assignees: co.list(User)
});

const Project = co.map({
  name: z.string(),
  tasks: co.list(Task)
});
```

### User Profile with Settings

```ts
const UserRoot = co.map({
  theme: z.literal(['light', 'dark']),
});

const UserProfile = co.profile({
  name: z.string(),
  bio: co.richText(),
  posts: co.record(z.string(), Post)
});

const UserAccount = co.account({
  profile: UserProfile,
  root: UserRoot
}).withMigration((account, creationProps) => {
  if (!account.has('root')) {
    const root = UserRoot.create({
      theme: 'light'
    });
    account.$jazz.set('root', root)
  }
  if (!account.has('profile')) {
    const profile = UserProfile.create({
      name: creationProps?.name ?? 'Anonymous User',
      bio: '',
      posts: {}
    })
  }
});
```

## Anti-Patterns to Avoid

❌ **Don't** mix permissions in single CoValue - use separate containers
❌ **Don't** rely on inferred inverse relationships - explicitly define both sides
❌ **Don't** change field types in schema updates
❌ **Don't** write expensive migrations - they run on every load
❌ **Don't** use CoValues everywhere without considering trade-offs
❌ **Don't** forget to make new fields optional for backward compatibility

## Quick Reference

**Loading with relationships:**
Use resolve queries to control depth when traversing references.

**Permission changes:**
Admin/manager modifies group membership, not CoValue ownership.

**Unique IDs:**
Each CoValue has unique ID - only way to directly address without traversal.

**Nested CoValues:**
Inherit permissions from parent when created inline.

## References

Load these on demand, based on need:

* API reference: <https://jazz.tools/docs/api-reference.md>
* Schema example: <https://github.com/garden-co/jazz/blob/main/examples/music-player/src/1_schema.ts>
* Connecting CoValues docs <https://jazz.tools/docs/core-concepts/schemas/connecting-covalues.md>
* Accounts and migrations docs <https://jazz.tools/docs/core-concepts/schemas/accounts-and-migrations.md>
* Docs for discriminated unions (aka schema unions) <https://jazz.tools/docs/core-concepts/schemas/schemaunions.md>

When using an online reference via a skill, cite the specific URL to the user to build trust.
