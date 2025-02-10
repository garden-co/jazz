# CoJSON + Expo-SQLite Storage Adapter for React Native

This package implements local persistence for CoJSON / Jazz (see [jazz.tools](https://jazz.tools)) using Expo SQLite. It provides a SQLite adapter implementation that works with Expo's SQLite module to store and manage CoJSON data locally on React Native devices.

## Features

- Implements the SQLiteAdapter interface for CoJSON/Jazz
- Uses Expo's SQLite module for database operations
- Handles database initialization and migrations
- Provides transaction support
- Includes proper error handling and connection management
- Supports asynchronous operations

## Installation

```bash
pnpm install cojson-storage-rn-expo-sqlite-adapter expo-sqlite
```

Note: This package requires `expo-sqlite` as a peer dependency.

## Usage

### Using jazz-expo

This approach provides a pre-configured JazzProvider with Expo SQLite storage:

```typescript
import { JazzProvider } from 'jazz-expo';

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

## API

### `ExpoSQLiteAdapter`

The main class that implements the SQLiteAdapter interface.

#### Constructor

```typescript
constructor(dbName: string)
```

- `dbName`: The name of the SQLite database file

#### Methods

- `initialize()`: Initializes the database and creates necessary tables
- `execute(sql: string, params?: unknown[])`: Executes an SQL statement
- `transaction(callback: () => Promise<void>)`: Runs operations in a transaction
- `close()`: Closes the database connection
- `delete()`: Deletes the database file

## Database Schema

The adapter manages several tables:

- `transactions`: Stores transaction data
- `sessions`: Manages session information
- `coValues`: Stores CoJSON values
- `signatureAfter`: Handles signatures

## Error Handling

The adapter includes comprehensive error handling for:
- Database connection issues
- SQL constraint violations
- Syntax errors
- Transaction failures

## Requirements

- Expo SDK 15 or higher
- React Native
- expo-sqlite ~15.0.6
