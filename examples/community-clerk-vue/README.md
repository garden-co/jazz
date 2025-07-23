# Clerk authentication example with Jazz and Vue

This is an example of how to use clerk authentication with Jazz in a Vue.js application.

Live version: [](Todo)

## Getting started

You can either

1. Clone the jazz repository, and run the app within the monorepo.
2. Or create a new Jazz project using this example as a template.

### Using the example as a template

Create a new Jazz project, and use this example as a template.

```bash
npx create-jazz-app@latest clerk-vue-app --example community-clerk-vue
```

Go to the new project directory.

```bash
cd clerk-vue-app
```

Rename .env.example to .env

```bash
mv .env.example .env
```

Update `VITE_CLERK_PUBLISHABLE_KEY` with your [Publishable Key](https://clerk.com/docs/deployments/clerk-environment-variables#clerk-publishable-and-secret-keys) from Clerk.

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
cd jazz/examples/community-clerk-vue/
```

Rename .env.example to .env

```bash
mv .env.example .env
```

Update `VITE_CLERK_PUBLISHABLE_KEY` with your [Publishable Key](https://clerk.com/docs/deployments/clerk-environment-variables#clerk-publishable-and-secret-keys) from Clerk.

Start the dev server.

```bash
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173) with your browser to see the result.

## Questions / problems / feedback

If you have feedback, let us know on [Discord](https://discord.gg/utDMjHYg42) or open an issue or PR to fix something that seems wrong.
