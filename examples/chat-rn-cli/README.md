# 🎷 Jazz + React Native + Demo Auth

## 🚀 How to Run

### 1. Inside the Workspace Root

First, install dependencies for the project:

```bash
pnpm i
```

### 2. Inside the `examples/chat-rn-cli` Directory

Next, navigate to the specific example project and run the following commands:

```bash
pnpm pods
pnpm ios
```

This will set up and launch the app on iOS. For Android, you can skip `pnpm pods` and replace the last command with `pnpm android`.

## Creation

This was created using the following command:

```bash
bunx @react-native-community/cli init chat_rn_cli --version 0.78 --install-pods true --skip-git-init true --package-name com.jazz.chatrncli --directory chat-rn-cli
```

Then delete `bun.lock`, change package name in `package.json`, and begin build instructions above.
