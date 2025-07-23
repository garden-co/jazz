# `community-jazz-vue`

Vue bindings for Jazz (see [jazz.tools](https://jazz.tools)), a framework for distributed state.

## Quick Start

### 1. Install
```bash
npm install community-jazz-vue
```

### 2. Import CSS (Required for Inspector)
```typescript
// In your main.ts or main.js
import 'community-jazz-vue/dist/community-jazz-vue.css'
```

### 3. Use Jazz Inspector
```vue
<template>
  <JazzInspector position="bottom-right" />
</template>

<script setup>
import { JazzInspector } from 'community-jazz-vue/inspector'
</script>
```

## Documentation

- [Inspector Documentation](./src/inspector/README.md)
- [Canonical Vue Implementation](./src/inspector/CANONICAL_VUE_IMPLEMENTATION.md)
