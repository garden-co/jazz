# Jazz UI

Shared UI component library for Jazz projects built on [shadcn/ui](https://ui.shadcn.com/).

## Installation

```bash
pnpm add jazz-ui react react-dom tailwindcss
```

## Usage

Import components and styles:

```tsx
import { Button, Input, Label } from "jazz-ui";
import "jazz-ui/styles";

function MyComponent() {
  return (
    <div>
      <Label htmlFor="email">Email</Label>
      <Input id="email" type="email" placeholder="Enter your email" />
      <Button>Submit</Button>
    </div>
  );
}
```

## Theme

- **OKLCH color space** for perceptually uniform colors
- **Fonts**: Inter (sans), Google Sans Code (mono)
- **Tailwind v4** with CSS-first configuration
- **Dark mode**: Add `dark` class to root element

Import the shared Tailwind preset:

```ts
import { tailwindPreset } from "jazz-ui/config";

export default {
  presets: [tailwindPreset],
  content: [
    "./src/**/*.{js,ts,jsx,tsx}",
    "./node_modules/jazz-ui/dist/**/*.js",
  ],
} satisfies Config;
```

## Utilities

```tsx
import { cn } from "jazz-ui";

<div className={cn("base-class", condition && "conditional-class")} />;
```

## Development

```bash
pnpm build        # Build package
pnpm dev          # Watch mode
pnpm format-and-lint:fix  # Format and lint
```

## License

MIT
