# Quint UI

## Installation

```sh
pnpm add quint-ui tailwindcss
```

then in your tailwind styles (ie. `styles.css`) add:
```css
@import "quint-ui/styles.css";
@source "../node_modules/quint-ui";
```
> [!TIP]
> There's no need to do
> ```css
> @import "tailwindcss";
> ```
> Quint does this for you.


### Usage

```tsx
import { Button } from "quint-ui";

<Button>Click me</Button>
```

## Development workflow

### In Quint

In this directory, run:
```sh
pnpm dev:docs
```
This starts a nextjs app you can use to write docs and/or test components.

### In example apps

In this directory, run:
```sh
pnpm dev
```
This starts typescript in watch mode. 

> [!IMPORTANT]
> Make sure quint-ui is installed in the example via 
> ```json
> "dependencies": {
>   "quint-ui": "workspace:*"
> }
> ```

## Building

### Docs website

```sh
pnpm build:docs
```

### Package

```sh
pnpm build
```

