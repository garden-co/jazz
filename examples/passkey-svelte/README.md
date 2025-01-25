# Passkey Authentication (Svelte)

This example app demonstrates how to implement passkey authentication in a Svelte application using Jazz.

## Features

This example showcases how to:
- Set up passkey authentication in a Svelte application
- Handle user registration with passkeys
- Manage authentication state
- Implement secure login/logout flows

## Getting started

You can either
1. Clone the jazz repository, and run the app within the monorepo.
2. Or create a new Jazz project using this example as a template.


### Using the example as a template

Create a new Jazz project, and use this example as a template.
```bash
npx create-jazz-app@latest --example passkey-svelte --project-name passkey-svelte
```

Go to the new project directory.
```bash
cd passkey-svelte
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
cd jazz/examples/passkey-svelte/
```

Start the dev server.
```bash
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173) with your browser to see the result.

## Learn More

- [Jazz Documentation](https://jazz.tools/docs/svelte)
- [WebAuthn API](https://developer.mozilla.org/en-US/docs/Web/API/Web_Authentication_API)
- [Svelte Documentation](https://svelte.dev)
