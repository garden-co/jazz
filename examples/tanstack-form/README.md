# Jazz Form Example - Svelte

A comprehensive form example built with Svelte and Jazz, demonstrating reactive form handling, validation, and real-time data synchronization.

## Overview

This example showcases a bubble tea ordering system with the following features:

- **Create Orders**: Build new bubble tea orders with base tea, add-ons, delivery date, and special instructions
- **Edit Orders**: Modify existing orders with real-time preview
- **Form Validation**: Client-side validation with error messages
- **Real-time Sync**: Jazz CoValues for real-time data synchronization
- **Responsive Design**: Modern UI with Tailwind CSS

## Key Technologies

- **Svelte 5.x**: Modern reactive UI framework with runes
- **SvelteKit**: Full-stack Svelte framework
- **Jazz Tools**: Real-time collaborative data layer
- **TypeScript**: Full type safety
- **Tailwind CSS**: Utility-first CSS framework

## Architecture

### State Management
Instead of external form libraries, this example uses Svelte's native reactivity:

- `$state` - For mutable reactive state
- `$derived` - For computed values (like form validity)
- `$effect` - For side effects and lifecycle management

### Form Pattern
```svelte
<script>
  // Reactive form state
  let formData = $state({
    baseTea: '',
    addOns: [],
    deliveryDate: new Date(),
    withMilk: false,
    instructions: ''
  });

  // Derived validation
  const isValid = $derived(
    formData.baseTea && formData.deliveryDate
  );

  // Event handlers
  function handleSubmit(event) {
    // Handle form submission
  }
</script>
```

### Jazz Integration
The example demonstrates Jazz CoValues for:
- **Account Management**: User authentication and profile
- **Data Persistence**: Bubble tea orders stored as CoMaps
- **Real-time Sync**: Changes sync across sessions
- **Collaborative Features**: Shared data structures

## Getting Started

```bash
# Install dependencies
pnpm install

# Start development server
pnpm dev

# Build for production
pnpm build
```

## Project Structure

```
src/
├── lib/
│   ├── components/
│   │   ├── CreateOrder.svelte      # New order creation
│   │   ├── EditOrder.svelte        # Edit existing orders
│   │   ├── Orders.svelte           # Order list view
│   │   ├── OrderFormWithSaveButton.svelte  # Main form component
│   │   ├── OrderThumbnail.svelte   # Order preview
│   │   └── LinkToHome.svelte       # Navigation
│   ├── schema.ts                   # Jazz CoValue definitions
│   └── apiKey.ts                   # Jazz API configuration
├── routes/
│   ├── +layout.svelte              # App layout with Jazz provider
│   └── +page.svelte                # Main app with routing
└── app.html                        # HTML template
```

## Migration from React

This Svelte version was migrated from a React + TanStack Form implementation. Key differences:

| React | Svelte |
|-------|--------|
| `useForm` hook | `$state` runes |
| `form.Field` components | Native form elements |
| `form.Subscribe` | `$derived` reactives |
| Event handlers via props | Native Svelte event handling |
| TanStack Form validation | Custom validation functions |

The Svelte implementation is more concise while maintaining the same functionality and type safety.

## Features Demonstrated

- ✅ **Form State Management**: Reactive state with validation
- ✅ **Real-time Preview**: Live updates as user types
- ✅ **Date Handling**: Proper Date ↔ string conversion for inputs
- ✅ **Array Fields**: Dynamic add-on selection
- ✅ **Error Handling**: Field-level validation messages
- ✅ **Submit State**: Loading states and form submission
- ✅ **Jazz Integration**: CoValue creation and updates
- ✅ **TypeScript**: Full type safety throughout

## Learn More

- [Svelte Documentation](https://svelte.dev/docs)
- [SvelteKit Documentation](https://kit.svelte.dev/docs)
- [Jazz Documentation](https://jazz.tools/docs)
- [Tailwind CSS](https://tailwindcss.com/docs) 
