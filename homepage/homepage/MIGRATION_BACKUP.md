# Tailwind v3 to v4 Migration - Pre-Migration State

## Package Versions (Before Migration)

```json
{
  "devDependencies": {
    "autoprefixer": "^10",
    "tailwindcss": "^3",
    "prettier-plugin-tailwindcss": "^0.7.1"
  },
  "dependencies": {
    "tailwind-merge": "^1.14.0",
    "tailwindcss-animate": "^1.0.7"
  }
}
```

## PostCSS Configuration (Before)

File: `postcss.config.cjs`

```javascript
module.exports = {
  plugins: {
    tailwindcss: {},
    "@csstools/postcss-oklab-function": { preserve: true },
    autoprefixer: {},
  },
};
```

## Tailwind Configuration (Before)

File: `tailwind.config.ts`

- Uses v3 preset from `@garden-co/design-system`
- darkMode: `["class"]`
- Dynamic safelist with arbitrary values from `pingColorMap`

## CSS Setup (Before)

File: `app/globals.css`

- Uses `@tailwind base`, `@tailwind components`, `@tailwind utilities`
- Contains `@apply` directives in `@layer base`
- Custom CSS variables for theme colors
- Imports design-system CSS

## Key Features to Test After Migration

### Pages
- Homepage (/, hero section, features, code tabs)
- Docs pages (/docs/*)
- Examples page (/examples)
- Cloud/Status page (/cloud, /status)

### Components with Complex Styling
- `components/cloud/latencyMap.tsx` - arbitrary color values
- `components/LatencyChart.tsx` - dynamic styling
- `components/home/CodeTabs.tsx` - grid layouts
- Navigation and search components
- Code syntax highlighting (shiki)

### Theme System
- Light/dark mode switching
- CSS variable resolution
- Color transformations (oklch/lch functions)

## Critical Dependencies
- Design-system preset (still on Tailwind v3)
- PostCSS oklab-function plugin (for color support)
- tailwindcss-animate plugin
- @tailwindcss/typography (in design-system)

