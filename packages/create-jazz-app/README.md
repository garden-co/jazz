# Create Jazz App

🎷 A modern CLI tool to scaffold Jazz applications with your favorite framework and authentication method.

## Features

- 🚀 Quick start with popular frameworks (React, Svelte, Next.js, React Native)
- 🔐 Multiple authentication options (Demo, Passkey, Clerk, etc.)
- 📦 Support for various package managers (npm, yarn, pnpm, bun, deno)
- 💅 Beautiful CLI interface with interactive prompts
- ⚡️ Zero-config setup process

## Usage

You can create a new Jazz app in two ways:

### Interactive mode

Simply run:

```bash
npx create-jazz-app@latest
```

Then follow the interactive prompts to select your:

- Framework and authentication combination
- Package manager
- Project name

### Command line mode

Or specify all options directly:

```bash
npx create-jazz-app@latest my-app --starter react-passkey-auth --package-manager npm
```

### Start with an example app

You can use any of our example apps as a template for your own app, instead of choosing one of the starters.

Use the `--example` parameter, and pass
the directory name of the example app found [here](https://github.com/garden-co/jazz/tree/main/examples).

```bash
npm create jazz-app@latest --example chat
```

More starters coming soon! Check the help menu (`create-jazz-app --help`) for the latest list.

## System requirements

- Node.js 14.0.0 or later
- Package manager of your choice (npm, yarn, pnpm, bun, or deno)

## What happens when you run it?

1. 🎭 Prompts for your preferences (or uses command line arguments)
2. 📥 Clones the appropriate starter template
3. 📦 Updates dependencies to their latest versions
4. ⚙️ Installs all required packages
5. 🎉 Sets up your project and provides next steps

## License

MIT

---

Made with ♥️ by the Jazz team
