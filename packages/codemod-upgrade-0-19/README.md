# Jazz Codemods - 0.19 Upgrade

This repository contains codemods for upgrading Jazz projects to version 0.19.

## What are Codemods?

Codemods are automated refactoring tools that help you upgrade your codebase. 

They rewrite your code using AST modification.

We use `ts-morph` under the hood, to target specific Jazz types in our migrations.

Codemods modify your code, not always in the correct way.
Be sure that everything important is committed in your repo and there aren't unstaged changes that might get lost in the process.

## What This Codemod Does

The Jazz 0.19 upgrade codemod performs the following transformations:

1. **Renames `useCoStateWithSelector` to `useCoState`** - The hooks are now polymorphic and support the selector pattern natively
2. **Renames `useAccountWithSelector` to `useAccount`** - The hooks are now polymorphic and support the selector pattern natively

These hooks maintain the same API and functionality, they just have simplified names since all hooks now support the selector pattern.

## Usage

### Running the Jazz 0.19 Codemod

To run the Jazz 0.19 codemod, first upgrade the jazz-tools version to 0.19 and then execute:

```bash
npx jazz-tools-codemod-0-19
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
npx jazz-tools-codemod-0-19 src/
```

## Examples

### Before (0.18)

```typescript
import { useCoStateWithSelector, useAccountWithSelector } from "jazz-tools/react";

function MyComponent() {
  const todo = useCoStateWithSelector(TodoItem, todoId, {
    resolve: { text: true },
    select: (todo) => todo,
  });

  const todos = useAccountWithSelector(MyAccount, {
    resolve: { root: { todos: { $each: true } } },
    select: (me) => me?.root.todos,
  });

  // ...
}
```

### After (0.19)

```typescript
import { useCoState, useAccount } from "jazz-tools/react";

function MyComponent() {
  const todo = useCoState(TodoItem, todoId, {
    resolve: { text: true },
    select: (todo) => todo,
  });

  const todos = useAccount(MyAccount, {
    resolve: { root: { todos: { $each: true } } },
    select: (me) => me?.root.todos,
  });

  // ...
}
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

