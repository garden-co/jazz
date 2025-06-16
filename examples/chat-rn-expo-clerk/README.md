# 🎷 Jazz + Expo + `expo-router` + Clerk Auth

## 🚀 How to Run

### 1. Inside the Workspace Root

First, install dependencies and build the project:

```bash
pnpm i
mv .env.example .env
pnpm run build
```

Don't forget to update `VITE_CLERK_PUBLISHABLE_KEY` in `.env` with your [Publishable Key](https://clerk.com/docs/deployments/clerk-environment-variables#clerk-publishable-and-secret-keys) from Clerk.

### 2. Inside the `examples/chat-rn-expo-clerk` Directory

Next, navigate to the specific example project and run the following commands:

```bash
pnpm expo prebuild
pnpx pod-install
pnpm expo run:ios
```

This will set up and launch the app on iOS. For Android, you can replace the last command with `pnpm expo run:android`.
