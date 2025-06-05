# Jazz RichText Tiptap Example

A demonstration of collaborative rich text editing with Jazz, React, and Tiptap.

Live version: [https://richtext-tiptap.demo.jazz.tools](https://richtext-tiptap.demo.jazz.tools)

## Overview

This example shows how to implement collaborative rich text editing using:

- **Jazz** for real-time synchronization
- **CoRichText** for collaborative rich text data structures
- **Tiptap** for the rich text editor UI
- **React** for the component framework

The example features:

- Side-by-side plaintext and rich text editors
- Real-time collaboration across devices
- Persistent document storage

## Getting started

You can either

1. Clone the jazz repository, and run the app within the monorepo.
2. Or create a new Jazz project using this example as a template.

### Using the example as a template

Create a new Jazz project, and use this example as a template.

```bash
npx create-jazz-app@latest richtext-tiptap-app --example richtext-tiptap
```

Go to the new project directory.

```bash
cd richtext-tiptap-app
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
cd jazz/examples/richtext-tiptap/
```

Start the dev server.

```bash
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173) with your browser to see the result.

## How it works

This example demonstrates two key functionalities:

1. **CoRichText** - Jazz's collaborative rich text data structure
2. **Rich text integration** - Using Tiptap with Jazz

### Key components

- `src/schema.ts` - Defines the data model, including the `bio` field using CoRichText
- `src/Editor.tsx` - Implements both plaintext and rich text editor views
- `jazz-richtext-tiptap` - Provides the plugin that connects Jazz to Tiptap

### Implementation details

The example shows how to:

- Create and store CoRichText values
- Set up a plaintext editor with CoRichText
- Set up a Tiptap editor with a Jazz plugin

## Extending this example

You can extend this example by:

- Adding formatting options to the rich text toolbar
- Adding multiple collaborative documents
- Building document history or versioning

## Configuration: sync server

By default, the app uses [Jazz Cloud](https://jazz.tools/cloud) (`wss://cloud.jazz.tools`) - so cross-device use, invites and collaboration should just work.

You can also run a local sync server by running`npx jazz-run sync`, and setting the `sync` parameter of`JazzProvider` in [./src/main.tsx](./src/main.tsx) to`{ peer: "ws://localhost:4200" }`.

## Learn more

To learn more about Jazz's collaborative text capabilities:

- [Jazz documentation](https://jazz.tools/docs)
- [CoText documentation](https://jazz.tools/docs/using-covalues/cotext)
- [Tiptap documentation](https://tiptap.dev/docs)

## Questions / problems / feedback

If you have feedback, let us know on [Discord](https://discord.gg/utDMjHYg42) or open an issue or PR to fix something that seems wrong.
