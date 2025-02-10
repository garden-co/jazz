# CoJSON Storage SQLite for React Native

This package provides the core SQLite implementation for CoJSON / Jazz (see [jazz.tools](https://jazz.tools)) in React Native applications. It serves as the foundation for specific SQLite adapters like `jazz-react-native` (uses to op-sqlite) and `jazz-expo` (uses expo-sqlite).

## Overview

This package defines the core interfaces and implementations for SQLite-based persistence in React Native, including:

- SQLite adapter interface definition
- Core SQLite client implementation
- Sync manager integration
- Database schema and migration management

## Features

- Defines the standard SQLiteAdapter interface
- Provides core SQLite client implementation
- Handles sync message processing
- Manages database operations and transactions
- Supports both synchronous and asynchronous operations
- Implements proper error handling and connection management

## Installation

```bash
pnpm install cojson-storage-rn-sqlite
```

Note: This is a base package. For actual usage, you should install one of the concrete implementations:
- `jazz-expo` for Expo SQLite
- `jazz-react-native` for op-sqlite

## Architecture

### SQLiteAdapter Interface

The core interface that concrete implementations must implement:

```typescript
interface SQLiteAdapter {
  initialize(): Promise<void>;
  execute(sql: string, params?: unknown[]): Promise<SQLResult>;
  executeSync?(sql: string, params?: unknown[]): { rows: SQLRow[] };
  transaction(callback: () => Promise<void>): Promise<void>;
}
```

### SQLiteClient

Implements the database client interface with features like:
- CoValue management
- Session handling
- Transaction processing
- Signature verification

### SQLiteReactNative

Provides the core React Native SQLite implementation with:
- Sync manager integration
- Message processing
- Peer management

## Database Schema

The package manages several core tables:

- `transactions`: Stores transaction data
- `sessions`: Manages session information
- `coValues`: Stores CoJSON values
- `signatureAfter`: Handles signatures

## For Adapter Implementers

If you're implementing a new SQLite adapter:

1. Implement the `SQLiteAdapter` interface
2. Use the provided `SQLiteClient` for database operations
3. Integrate with `SQLiteReactNative` for sync functionality

Example:

```typescript
import { SQLiteAdapter, SQLiteReactNative } from 'cojson-storage-rn-sqlite';

class MyCustomAdapter implements SQLiteAdapter {
  // Implement required methods
}

// Use with SQLiteReactNative
const storage = new SQLiteReactNative(
  new MyCustomAdapter(),
  fromLocalNode,
  toLocalNode
);
```

## Requirements

- React Native
- A concrete SQLite implementation (Expo SQLite or op-sqlite)

