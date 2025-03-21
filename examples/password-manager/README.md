# Password manager example with Jazz and React

Live version: https://passwords-demo.jazz.tools

![Password Manager Screenshot](demo.png "Screenshot")

## Getting started

You can either
1. Clone the jazz repository, and run the app within the monorepo.
2. Or create a new Jazz project using this example as a template.


### Using the example as a template

Create a new Jazz project, and use this example as a template.
```bash
npx create-jazz-app@latest password-manager-app --example password-manager
```

Go to the new project directory.
```bash
cd password-manager-app
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
cd jazz/examples/password-manager/
```

Start the dev server.
```bash
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173) with your browser to see the result.

## Structure

- [`src/components`](./src/components/): UI components
- [`src/1_schema.ts`](./src/1_schema.ts): Jazz data model
- [`src/2_main.tsx`](./src/2_main.tsx): Main App component wrapped in `<JazzProvider>`
- [`src/3_vault.tsx`](./src/3_vault.tsx): Password Manager Vault page
- [`src/4_actions.tsx`](./src/4_actions.tsx): Jazz specific actions
- [`src/5_App.tsx`](./src/5_App.tsx): App router - also handles invite links
- [`src/types.ts`](./src/types.ts): shared types

## Walkthrough

### Main parts

1. Define the data model with CoJSON: [`src/1_schema.ts`](./src/1_schema.ts)

2. Wrap the App with the top-level provider `<JazzProvider>`: [`src/2_main.tsx`](./src/2_main.tsx)

3. Reactively render password items from folders inside a table, creating/sharing/deleting folders, creating/editing/deleting password items: [`src/3_vault.tsx`](./src/3_vault.tsx)

4. Implement Jazz specific actions: [`src/4_actions.tsx`](./src/4_actions.tsx)

5. Implement useAcceptInvite(): [`src/5_App.tsx`](./src/5_App.tsx)

## Questions / problems / feedback

If you have feedback, let us know on [Discord](https://discord.gg/utDMjHYg42) or open an issue or PR to fix something that seems wrong.

## Configuration: sync server

By default, the example app uses [Jazz Cloud](https://jazz.tools/cloud) (`wss://cloud.jazz.tools`) - so cross-device use, invites and collaboration should just work.

You can also run a local sync server by running `npx cojson-simple-sync` and adding the query param `?sync=ws://localhost:4200` to the URL of the example app (for example: `http://localhost:5173/?peer=ws://localhost:4200`), or by setting the `sync` parameter of the `<JazzProvider>` provider component in [./src/2_main.tsx](./src/2_main.tsx).
