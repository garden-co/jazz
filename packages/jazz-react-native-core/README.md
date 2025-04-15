# 🎷 Jazz React Native Core

This package provides the shared core functionality for React Native implementations of Jazz, supporting both framework-less React Native and Expo applications. It's designed to be a common foundation that the platform-specific packages (`jazz-react-native` and `jazz-expo`) build upon.

## Overview

Jazz React Native Core contains platform-agnostic functionality for React Native, including:

- Common React Native context management
- Base SQLite adapter interfaces
- Shared crypto functionality
- Core hooks and utilities

## Architecture

The Jazz React Native ecosystem is now structured as follows:

1. **jazz-react-native-core**: Common foundation (this package)
   - Shared interfaces and implementations
   - Base SQLite adapter interface

2. **jazz-react-native**: For framework-less React Native applications
   - Implements op-sqlite adapter
   - Uses MMKV for key-value storage
   - Directly imports jazz-react-native-core

3. **jazz-expo**: For Expo applications
   - Implements expo-sqlite adapter
   - Uses expo-secure-store for key-value storage
   - Directly imports jazz-react-native-core

## Usage

This package is typically not used directly by application developers. Instead, you should use either:

- `jazz-react-native` for framework-less React Native applications
- `jazz-expo` for Expo applications

These packages will automatically include the appropriate functionality from jazz-react-native-core.

## Requirements

```json
"react-native": "0.76.7",
"react": "18.3.1"
```

## For Contributors

If you're extending the Jazz framework, this package allows you to implement shared React Native functionality that works across both Expo and framework-less React Native applications.

### Testing

When adding new features, ensure they work in both environments:
- Framework-less React Native (using the jazz-react-native package)
- Expo applications (using the jazz-expo package)

## License

MIT
