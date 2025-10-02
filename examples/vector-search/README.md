# Vector search example with Jazz and Transformers.js

Live version: [https://vector-search.demo.jazz.tools](https://vector-search.demo.jazz.tools)

**A simple demo of vector search: personal journal app that lets you retrieve relatable journal entries.** 

Built with [Jazz](https://jazz.tools), [Transformers.js](https://huggingface.co/docs/transformers.js) and React.

## Getting started

You can either
1. Create a new Jazz project using this example as a template.
2. Or clone the `jazz` repository, and run the app within the monorepo.

### Using the example as a template

Create a new Jazz project, and use this example as a template.
```bash
npx create-jazz-app@latest journal-app --example vector-search
```

Go to the new project directory.
```bash
cd journal-app
```

Create a `.env` file and add an API key (available for free from https://dashboard.jazz.tools)
```
VITE_JAZZ_API_KEY=<your key>
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
cd jazz/examples/vector-search/
```

Create a `.env` file and add an API key (available for free from https://dashboard.jazz.tools)
```
VITE_JAZZ_API_KEY=<your key>
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

You can also run a local sync server by running `npx jazz-run sync`, and setting the `sync` parameter of `JazzReactProvider` in [./src/Main.tsx](./src/Main.tsx) to `{ peer: "ws://localhost:4200" }`.


---

## Acknowledgements

The test “journal dataset” is from the [paper](https://arxiv.org/abs/1903.07766), "Lemotif: An Affective Visual Journal Using Deep Neural Networks" by X. Li and Devi Parikh. It can be accessed online [here](https://github.com/xaliceli/lemotif).
