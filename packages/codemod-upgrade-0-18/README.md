# Jazz Codemods

This repository contains codemods for upgrading Jazz projects to newer versions. Codemods are automated tools that help migrate your codebase to be compatible with newer versions of Jazz.

## What are Codemods?

Codemods are automated refactoring tools that help you upgrade your codebase. 

They rewrite your code using AST modification.

We use `ts-morph` under the hood, to target specific Jazz types in our migrations.

Codemods modify your code, not always in the correct way.
Be sure that everything important is committed in your repo and there aren't unstaged changes that might get lost in the process.

## Available Codemods

### Jazz 0.18 Upgrade

The main codemod available in this repository is for upgrading to Jazz 0.18.

## Usage

### Running the Jazz 0.18 Codemod

To run the Jazz 0.18 codemod upgrade the jazz-tools version to 0.18 and then execute:

```bash
npx jazz-tools-codemod-0-18
```

### How it Works

By default, the codemod will:

1. **Try to use the tsconfig.json in the current working directory** to resolve project files
2. **Fall back to glob search** if no tsconfig.json is found or if it can't resolve the project structure

## Prerequisites

- Node.js (version 14 or higher)
- npm, yarn, or pnpm

## Installation

The codemod is available as an npm package and can be run directly with npx, so no local installation is required.

## Running on Specific Files

If you want to run the codemod on specific files or directories, you can pass them as arguments:

```bash
npx jazz-tools-codemod-0-18 src/
```

## Contributing

If you find issues with the codemod or want to contribute improvements, please:

1. Open an issue describing the problem
2. Fork the repository
3. Make your changes
4. Submit a pull request

## License

MIT

## Support

For issues related to the codemod itself, please open an issue in this repository. For general Jazz support, please refer to the official Jazz documentation.
