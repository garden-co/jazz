# Jazz Stress Test

A multi-scenario stress test app for testing Jazz performance.

## Scenarios

### Todo Projects
Generate random todo projects with configurable task counts for testing list rendering and sync performance.

### Pixel Grid
Generate NxN pixel grids with random colors and configurable payload sizes for testing canvas rendering and data loading.

## Usage

```bash
# Start the development server
pnpm dev

# Build for production
pnpm build
```

## Sync Server

By default, connects to `ws://localhost:4200`. You can:
- Pass a `?sync=wss://your-server.com` query parameter
- The app remembers recent connections in localStorage
