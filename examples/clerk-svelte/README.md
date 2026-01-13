# Clerk authentication example with Jazz and Svelte

This is an example of how to use Clerk authentication with Jazz using SvelteKit.

## Getting started

You can either

1. Clone the jazz repository, and run the app within the monorepo.
2. Or create a new Jazz project using this example as a template.

### Using the example as a template

Create a new Jazz project, and use this example as a template.

```bash
npx create-jazz-app@latest clerk-app --example clerk-svelte
```

Go to the new project directory.

```bash
cd clerk-app
```

Rename .env.example to .env

```bash
mv .env.example .env
```

Update `VITE_CLERK_PUBLISHABLE_KEY` with your [Publishable Key](https://clerk.com/docs/deployments/clerk-environment-variables#clerk-publishable-and-secret-keys) from Clerk.

Update `VITE_JAZZ_API_KEY` with an API key from https://dashboard.jazz.tools

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
cd jazz/examples/clerk-svelte/
```

Rename .env.example to .env

```bash
mv .env.example .env
```

Update `VITE_CLERK_PUBLISHABLE_KEY` with your [Publishable Key](https://clerk.com/docs/deployments/clerk-environment-variables#clerk-publishable-and-secret-keys) from Clerk.

Update `VITE_JAZZ_API_KEY` with an API key from https://dashboard.jazz.tools

Start the dev server.

```bash
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173) with your browser to see the result.

## Questions / problems / feedback

If you have feedback, let us know on [Discord](https://discord.gg/utDMjHYg42) or open an issue or PR to fix something that seems wrong.
