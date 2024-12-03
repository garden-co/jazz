# Chat example with Jazz and Vue

## Installing & running the example locally

(This requires `pnpm` to be installed, see [https://pnpm.io/installation](https://pnpm.io/installation))

Start by downloading the [jazz repository](https://github.com/garden-co/jazz):
```bash
npx degit gardencmp/jazz jazz
```

Go to the todo-vue example directory:
```bash
cd jazz/examples/chat-vue
```

Install and build dependencies:
```bash
pnpm i && npx turbo build
```

Start the dev server:
```bash
pnpm dev
```

## Questions / problems / feedback

If you have feedback, let us know on [Discord](https://discord.gg/utDMjHYg42) or open an issue or PR to fix something that seems wrong.

## Configuration: sync server

By default, the example app uses [Jazz Cloud](https://jazz.tools/cloud) (`wss://cloud.jazz.tools`) - so cross-device use, invites and collaboration should just work.

You can also run a local sync server by running `npx jazz-run sync` and adding the query param `?sync=ws://localhost:4200` to the URL of the example app (for example: `http://localhost:5173/?peer=ws://localhost:4200`), or by setting the `sync` parameter of `JazzProvider` in [./src/main.ts](./src/main.ts).
