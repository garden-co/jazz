# Vue Jazz Inspector

A Vue.js implementation of the Jazz Inspector that provides 1:1 visual and functional parity with the React version.

## Overview

The Vue Jazz Inspector allows you to explore and debug Jazz CoValues in your Vue application. It provides a comprehensive interface for inspecting accounts, groups, lists, maps, and other Jazz data structures.

## Installation

The Vue Inspector is included with `community-jazz-vue`:

```bash
npm install community-jazz-vue
```

## Quick Start

### Step 1: Import CSS (Required)

**⚠️ Important**: You must import the CSS file for the inspector to display correctly:

```typescript
// In your main.ts or main.js
import 'community-jazz-vue/dist/community-jazz-vue.css'
```

**Why is this needed?** Unlike the React version which uses Goober (CSS-in-JS), the Vue version uses pre-compiled CSS for better performance and smaller bundle size. This requires a one-time CSS import.

### Step 2: Use the Inspector

#### Method 1: Using the Canonical Vue Inspector (Recommended)

```vue
<template>
  <div>
    <!-- Your app content -->

    <!-- Jazz Inspector - shows floating button when closed -->
    <JazzInspector position="right" />
  </div>
</template>

<script setup lang="ts">
import { JazzInspector } from 'community-jazz-vue/inspector'
</script>
```

#### Method 2: Using the Legacy JSX Inspector (Deprecated)

```vue
<template>
  <div>
    <!-- Your app content -->

    <!-- Controlled Inspector -->
    <JazzInspectorInternal
      v-if="showInspector && localNode"
      position="right"
      :localNode="localNode"
      :accountId="accountId"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { Account } from 'jazz-tools'
import { useJazzContext } from 'community-jazz-vue'
import { JazzInspectorInternal } from 'community-jazz-vue/inspector'

const context = useJazzContext<Account>()
const showInspector = ref(false)

const localNode = computed(() => {
  const ctx = context.value
  if (!ctx) return undefined
  return ctx.node
})

const accountId = computed(() => {
  const ctx = context.value
  if (!ctx) return undefined
  const me = "me" in ctx ? ctx.me : undefined
  return me?._raw?.id
})
</script>
```

## Available Components

### Main Inspector Components

- **`JazzInspector`** - Complete inspector with floating button (recommended)
- **`JazzInspectorInternal`** - Inspector panel without floating button (for custom integration)

### UI Components

All UI components use identical styling to the React version:

- **`Button`** - Primary, secondary, link, and plain button variants
- **`Input`** - Text input with label and validation
- **`Select`** - Dropdown select with options
- **`Badge`** - Small status/type indicators
- **`Card`** / `CardHeader` / `CardBody` - Content containers
- **`Grid`** - Responsive grid layout (1, 2, or 3 columns)
- **`Heading`** - Styled headings
- **`Text`** - Styled text with variants (muted, strong, small, mono)
- **`Table`** components - Complete table implementation
- **`Icon`** - Icon system with built-in icons

### Utility Components

- **`GlobalStyles`** - CSS variables and global styles
- **`InspectorButton`** - Floating inspector toggle button

## Props

### JazzInspector

```typescript
interface JazzInspectorProps {
  position?: 'right' | 'left' | 'top right' | 'top left' | 'bottom right' | 'bottom left'
}
```

### JazzInspectorInternal

```typescript
interface JazzInspectorInternalProps {
  position?: 'right' | 'left' | 'top right' | 'top left' | 'bottom right' | 'bottom left'
  localNode?: LocalNode
  accountId?: CoID<RawAccount>
}
```

## Features

✅ **1:1 Parity with React** - Identical styling and functionality
✅ **Bottom-Sheet Layout** - Practical full-width layout for consuming large amounts of data
✅ **CoValue Exploration** - Browse accounts, groups, maps, lists, and streams
✅ **Navigation** - Breadcrumb navigation with back/forward support for long IDs
✅ **Search** - Direct CoValue ID input and inspection
✅ **Role Display** - Shows permissions and ownership information
✅ **Raw Data View** - JSON view with copy functionality
✅ **Responsive Design** - Optimized for data consumption on all screen sizes
✅ **Dark Mode** - Automatic dark/light theme support
✅ **TypeScript Support** - Full type safety

## Examples

### Basic Usage

```vue
<template>
  <JazzProvider>
    <div class="app">
      <h1>My Jazz App</h1>

      <!-- Inspector will show as floating button -->
      <JazzInspector />
    </div>
  </JazzProvider>
</template>

<script setup lang="ts">
import { JazzProvider } from 'community-jazz-vue'
import { JazzInspector } from 'community-jazz-vue/inspector'
</script>
```

### Custom Integration

```vue
<template>
  <div class="app">
    <header>
      <h1>Jazz Admin Panel</h1>
      <button @click="toggleInspector">
        {{ inspectorOpen ? 'Hide' : 'Show' }} Inspector
      </button>
    </header>

    <main>
      <!-- Your app content -->
    </main>

    <!-- Custom inspector placement -->
    <JazzInspectorInternal
      v-if="inspectorOpen"
      position="bottom right"
      :localNode="localNode"
      :accountId="accountId"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { Account } from 'jazz-tools'
import { useJazzContext } from 'community-jazz-vue'
import { JazzInspectorInternal } from 'community-jazz-vue/inspector'

const inspectorOpen = ref(false)
const context = useJazzContext<Account>()

const localNode = computed(() => {
  const ctx = context.value
  return ctx?.node
})

const accountId = computed(() => {
  const ctx = context.value
  if (!ctx) return undefined
  const me = "me" in ctx ? ctx.me : undefined
  return me?._raw?.id
})

const toggleInspector = () => {
  inspectorOpen.value = !inspectorOpen.value
}
</script>
```

### Using Individual UI Components

```vue
<template>
  <Card>
    <CardHeader>
      <Heading>User Profile</Heading>
      <Badge>Active</Badge>
    </CardHeader>

    <CardBody>
      <Grid :cols="2">
        <div>
          <Text strong>Name:</Text>
          <Text>{{ user.name }}</Text>
        </div>

        <div>
          <Text strong>Email:</Text>
          <Text mono>{{ user.email }}</Text>
        </div>
      </Grid>

      <Button variant="primary" @click="editUser">
        Edit Profile
      </Button>
    </CardBody>
  </Card>
</template>

<script setup lang="ts">
import {
  Card,
  CardHeader,
  CardBody,
  Grid,
  Heading,
  Text,
  Badge,
  Button
} from 'community-jazz-vue/inspector'

// Your component logic
</script>
```

## Styling

The inspector uses CSS custom properties for theming. These are automatically included when you use any inspector component:

```css
:root {
  --j-primary-color: #146AFF;
  --j-text-color: #6b696a;
  --j-background: #FFFFFF;
  --j-border-color: #e5e3e4;
  /* ... and many more */
}

@media (prefers-color-scheme: dark) {
  :root {
    --j-text-color: #bbbaba;
    --j-background: #151414;
    --j-border-color: #2f2e2e;
    /* ... dark theme overrides */
  }
}
```

## Development Mode Only

The inspector automatically detects the environment and only renders in development mode:

```javascript
if (process.env.NODE_ENV === "production") return null;
```

For production debugging, you can force it to show by setting a development flag or using the `JazzInspectorInternal` component directly.

## Troubleshooting

### Inspector Not Showing

1. **Check Environment**: Inspector only shows in development mode
2. **Check Jazz Context**: Ensure you're within a `JazzProvider`
3. **Check Authentication**: User must be authenticated to use inspector

### TypeScript Errors

1. **Import Paths**: Use `community-jazz-vue/inspector` for inspector components
2. **Event Handlers**: Use proper Vue event syntax (`@click` not `onClick`)
3. **Props**: Use `class` prop instead of `className` for Vue components

### Styling Issues

1. **Z-Index**: Inspector uses `z-index: 999`, ensure no conflicts
2. **Automatic CSS**: Styles are automatically injected via JavaScript - no manual CSS import needed
3. **SSR Compatibility**: CSS injection works in browser environments only

## Migration from React

The Vue Inspector provides 1:1 parity with the React version:

**React:**
```jsx
import { JazzInspector } from "jazz-tools/inspector"

<JazzInspector position="right" />
```

**Vue:**
```vue
<script setup>
import { JazzInspector } from "community-jazz-vue/inspector"
</script>

<template>
  <JazzInspector position="right" />
</template>
```

## Contributing

The Vue Inspector is built using:

- **Vue 3** with Composition API
- **JSX** for component rendering (matching React structure)
- **Goober** for styled components (same as React version)
- **TypeScript** for type safety

When contributing, ensure:

1. Visual parity with React version
2. Identical functionality and behavior
3. Proper Vue patterns and conventions
4. TypeScript support maintained

## License

Same as community-jazz-vue package.
