# Jazz React Native Storage with Expo SQLite

This package provides a pre-configured Jazz Provider with Expo SQLite storage for React Native applications. It offers a batteries-included approach to using Jazz with Expo's SQLite implementation.

## Features

- Pre-configured JazzProvider with Expo SQLite storage
- Seamless integration with Jazz React Native
- Built-in state management for authentication
- Automatic handling of development mode peculiarities
- Clean session management and cleanup

## Installation

```bash
pnpm install jazz-react-native-storage-expo-sqlite expo-sqlite
```

## Usage

This package provides a simplified way to use Jazz with Expo SQLite storage. Simply import and use the pre-configured `JazzProvider`:

```typescript
import { JazzProvider } from 'jazz-react-native-storage-expo-sqlite';

function App() {
  return (
    <JazzProvider 
      auth={auth}
      peer="wss://cloud.jazz.tools/?key=you@example.com"
      AccountSchema={MyAppAccount}
    >
      <YourApp />
    </JazzProvider>
  );
}
```

The provider automatically:
- Sets up Expo SQLite storage
- Manages authentication state
- Handles session lifecycle
- Provides proper cleanup in development mode

### Props

- `auth`: Authentication configuration
- `peer`: WebSocket peer URL for sync
- `AccountSchema`: (Optional) Custom account schema
- `CryptoProvider`: (Optional) Custom crypto provider
- `children`: React components that need access to Jazz

### Guest Mode

You can also use the provider in guest mode:

```typescript
<JazzProvider 
  auth="guest"
  peer="wss://cloud.jazz.tools/?key=guest"
>
  <YourApp />
</JazzProvider>
```

## Under the Hood

This package:
1. Uses `cojson-storage-rn-expo-sqlite-adapter` for SQLite operations
2. Integrates with `jazz-react-native` for React Native specific features
3. Provides proper context management and cleanup
4. Handles development mode double-rendering gracefully

## Requirements

- React Native
- Expo SDK 15 or higher
- expo-sqlite ~15.0.6

## License

MIT License - see LICENSE.txt for details
