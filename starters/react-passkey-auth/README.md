# Jazz React starter with Tailwind and Passkey Auth

A minimal starter template for building apps with **[Jazz](https://jazz.tools)**, React, TailwindCSS, and Passkey Auth.

## Creating an app

Create a new Jazz app.
```bash
npx create-jazz-app@latest
```

## Running locally

Install dependencies:

```bash
npm i
# or
yarn
```

Then, run the development server:

```bash
npm run dev
# or
yarn dev
```

Open [http://localhost:5173](http://localhost:5173) with your browser to see the result.

## Learning Jazz

You can start by playing with the form, adding a new field in [./src/schema.ts](./src/schema.ts),
and seeing how easy it is to structure your data, and perform basic operations.

## Questions / problems / feedback

If you have feedback, let us know on [Discord](https://discord.gg/utDMjHYg42) or open an issue or PR to fix something that seems wrong.


## Configuration: sync server

By default, the React starter app uses [Jazz Cloud](https://jazz.tools/cloud) (`wss://cloud.jazz.tools`) - so cross-device use, invites and collaboration should just work.

You can also run a local sync server by running `npx jazz-run sync`, and setting the `sync` parameter of `JazzReactProvider` in [./src/app.tsx](./src/app.tsx) to `{ peer: "ws://localhost:4200" }`.
