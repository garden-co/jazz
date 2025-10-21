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

### 1. **Migrates `useAccount` Destructuring Pattern**

The old `useAccount` hook returned an object with `{ me, agent, logOut }`. Now it returns the account directly.

- Splits `{ me, agent, logOut } = useAccount()` into separate hook calls
- Replaces `account.me` property accesses with just `account`
- Adds `useAgent()` and `useLogOut()` imports when needed

### 2. **Renames Hook Functions**

- `useCoStateWithSelector` → `useCoState`
- `useAccountWithSelector` → `useAccount`

The hooks are now polymorphic and support the selector pattern natively.

### 3. **Migrates Error Handling**

- `$onError: null` → `$onError: 'catch'`

### 4. **Backwards-compatible Loading State Handling**

Updates `useAccount` and `useCoState` hooks to convert the new explicit loading states into `null | undefined`, maintaining backward compatibility in existing React components that use Jazz.

## Usage

### Running the Jazz 0.19 Codemod

To run the Jazz 0.19 codemod, first upgrade the jazz-tools version to 0.19 and then execute:

```bash
npx jazz-tools-codemod-0-19
```

For large projects, you may need to increase Node.js heap size:

```bash
NODE_OPTIONS="--max-old-space-size=8192" npx jazz-tools-codemod-0-19
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

## Example

This example demonstrates all the transformations the codemod performs:

**Before (0.18)**
```typescript
import { useCoStateWithSelector, useAccountWithSelector } from "jazz-tools/react";

function MyComponent({ todoId }) {
  const { me, logOut } = useAccount();
  const todos = useAccountWithSelector(MyAccount, {
    resolve: { root: { todos: { $each: { $onError: null } } } },
    select: (me) => me?.root.todos,
  });
  const todoText = useCoStateWithSelector(TodoItem, todoId, {
    resolve: { text: true },
    select: (todo) => todo.text,
  });
  
  // ...
}
```

**After (0.19)**
```typescript
import { useCoState, useAccount, useLogOut } from "jazz-tools/react";

function MyComponent({ todoId }) {
  const me = useAccount(undefined, {
    select: (me) => me.$isLoaded ? me : me.$jazz.loadingState === "loading" ? undefined : null
  });
  const logOut = useLogOut();
  
  const todos = useAccount(MyAccount, {
    resolve: { root: { todos: { $each: { $onError: 'catch' } } } },
    select: (me) => me.$isLoaded ? me.root.todos : me.$jazz.loadingState === "loading" ? undefined : null,
  });

  const todoText = useCoState(TodoItem, todoId, {
    resolve: { text: true },
    select: (todo) => todo.$isLoaded ? todo.text : todo.$jazz.loadingState === "loading" ? undefined : null,
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

