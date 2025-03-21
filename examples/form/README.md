# Form example with Jazz and React

Live version: [https://form-demo.jazz.tools](https://form-demo.jazz.tools)

This is a simple form example that shows you how to make a form for creating and editing a `CoValue`, 
called `BubbleTeaOrder`, with fields of different types such
as single-select, multi-select, date, text, and boolean.

To create a new `BubbleTeaOrder`, we create an empty order. Because `BubbleTeaOrder` has some
required fields, we can't create an empty `BubbleTeaOrder` directly. 

Instead, we create a `DraftBubbleTeaOrder`,
which has the same structure as `BubbleTeaOrder`, but with all fields set to `optional`.

When the user is ready to submit the order, we treat `DraftBubbleTeaOrder` as a "real order" by
converting it into a `BubbleTeaOrder`.

[See the full guide here.](https://jazz.tools/docs/react/design-patterns/form)

## Getting started

You can either
1. Clone the jazz repository, and run the app within the monorepo.
2. Or create a new Jazz project using this example as a template.


### Using the example as a template

Create a new Jazz project, and use this example as a template.
```bash
npx create-jazz-app@latest form-app --example form
```

Go to the new project directory.
```bash
cd form-app
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
cd jazz/examples/form/
```

Start the dev server.
```bash
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173) with your browser to see the result.

## Questions / problems / feedback

If you have feedback, let us know on [Discord](https://discord.gg/utDMjHYg42) or open an issue or PR to fix something that seems wrong.

## Configuration: sync server

By default, the example app uses [Jazz Cloud](https://jazz.tools/cloud) (`wss://cloud.jazz.tools`) - so cross-device use, invites and collaboration should just work.

You can also run a local sync server by running `npx jazz-run sync` and adding the query param `?sync=ws://localhost:4200` to the URL of the example app (for example: `http://localhost:5173/?peer=ws://localhost:4200`), or by setting the `sync` parameter of the `<JazzProvider>` provider component in [./src/main.tsx](./src/main.tsx).
