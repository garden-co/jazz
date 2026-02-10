# Chat example with Jazz and Svelte

## Getting started

You can either

1. Clone the jazz repository, and run the app within the monorepo.
2. Or create a new Jazz project using this example as a template.

### Using the example as a template

Create a new Jazz project, and use this example as a template.

```bash
npx create-jazz-app@latest chat-app --example chat-svelte
```

Go to the new project directory.

```bash
cd chat-app
```

Run the dev server.

```bash
npm run dev
```

### Using the monorepo

This requires `pnpm` to be installed, see [https://pnpm.io/installation](https://pnpm.io/installation).

Clone the jazz repository.

```bash
git clone https://github.com/garden-co/jazz.git
```

Install and build dependencies.

```bash
pnpm i && npx turbo build
```

Go to the example directory.

```bash
cd jazz/examples/chat-svelte/
```

Start the dev server.

```bash
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173) with your browser to see the result.

## Questions / problems / feedback

If you have feedback, let us know on [Discord](https://discord.gg/utDMjHYg42) or open an issue or PR to fix something that seems wrong.

## Configuration: sync server

By default, the example app uses [Jazz Cloud](https://dashboard.jazz.tools) (`wss://cloud.jazz.tools`) - so cross-device use, invites and collaboration should just work.

You can also run a local sync server by running `npx jazz-run sync`, and setting the `sync` parameter of `JazzSvelteProvider` in [./src/App.svelte](./src/App.svelte) to `{ peer: "ws://localhost:4200" }`.

## Appendix: Jazz Features

The below important Jazz-specific concepts are covered in this example:

### [CoValues](https://jazz.tools/docs/svelte/core-concepts/covalues/overview)
The app defines a rich schema using various `CoValue` types in [`src/lib/schema.ts`](./src/lib/schema.ts):
- **[`CoMap`](https://jazz.tools/docs/svelte/core-concepts/covalues/comaps)**: Used for `Message`, `ImageAttachment`, `FileAttachment`, and the `root` account data.
- **[`CoList`](https://jazz.tools/docs/svelte/core-concepts/covalues/colists)**: Used for the `Chat` (a list of messages).
- **[`CoFeed`](https://jazz.tools/docs/svelte/core-concepts/covalues/cofeeds)**: Used for message `reactions`.
- **[`CoPlainText`](https://jazz.tools/docs/svelte/core-concepts/covalues/cotexts)**: Used for collaborative message text.
- **[`FileStream`](https://jazz.tools/docs/svelte/core-concepts/covalues/filestreams)**: Used for `FileAttachment`.
- **[`ImageDefinition`](https://jazz.tools/docs/svelte/core-concepts/covalues/imagedef)**: Used for `ImageAttachment` and user profile avatars.
- **[`CoRecord`](https://jazz.tools/docs/svelte/core-concepts/covalues/comaps)**: Used for the `Canvas` (a record of strokes) and the account's `chats` index.
- **[`DiscriminatedUnion`](https://jazz.tools/docs/svelte/core-concepts/schemas/schemaunions)**: Used for the polymorphic `Attachment` type.

### [Permissions & Sharing](https://jazz.tools/docs/svelte/permissions-and-sharing/overview)
The app demonstrates Jazz's flexible permission system:
- **[Groups](https://jazz.tools/docs/svelte/permissions-and-sharing/overview)**: Creating and managing groups for chats and attachments.
- **[Schema-level Permissions](https://jazz.tools/docs/svelte/permissions-and-sharing/overview#defining-permissions-at-the-schema-level)**: Using `.withPermissions` on CoValue schemas to automate group creation and inheritance.
- **[Public sharing](https://jazz.tools/docs/svelte/permissions-and-sharing/sharing)**: Chat and canvas values specifically add `everyone` as a `writer`.
- **[Cascading permissions](https://jazz.tools/docs/svelte/permissions-and-sharing/cascading-permissions)**: Messages use `sameAsContainer` to inherit permissions from the `Chat` list.

### [Accounts & Profiles](https://jazz.tools/docs/svelte/core-concepts/schemas/accounts-and-migrations)
- **[Custom Account & Profile Schemas](https://jazz.tools/docs/svelte/core-concepts/schemas/accounts-and-migrations)**: Extending the base `Account` and `Profile` in `src/lib/schema.ts`.
- **[Migrations](https://jazz.tools/docs/svelte/core-concepts/schemas/accounts-and-migrations)**: Using `withMigration` to initialize account `root` structures.

### [Svelte Integration](https://jazz.tools/docs/svelte/api-reference)
Implementation of Jazz reactive classes for real-time state and account management:
- **[`AccountCoState`](https://jazz.tools/docs/svelte/core-concepts/subscription-and-loading#subscribe-to-the-current-users-account)**: Subscribing to current user data with deep resolution.
- **[`CoState`](https://jazz.tools/docs/svelte/core-concepts/subscription-and-loading#subscribe-to-covalues)**: Subscribing to collaborative state.
- **[Deep Loading](https://jazz.tools/docs/svelte/core-concepts/subscription-and-loading#using-resolve-queries)**: Resolving nested data structures like chat profiles and message attachments.
- **[`isAuthenticated` & `logOut`](https://jazz.tools/docs/svelte/key-features/authentication/overview)**: Managing authentication state via `AccountCoState`.

### [Sync & Developer Tools](https://jazz.tools/docs/svelte/core-concepts/sync-and-storage)
- **[Jazz Cloud Sync](https://jazz.tools/docs/svelte/core-concepts/sync-and-storage)**: Configured in `src/App.svelte` via `JazzSvelteProvider`.
- **[Jazz Inspector](https://jazz.tools/docs/svelte/tooling-and-resources/inspector)**: Integrated for easy data inspection.
