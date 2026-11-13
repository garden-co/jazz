---
"jazz-tools": patch
---

`createJazz` names the problem when schema.ts or permissions.ts reaches an ESM backend as a CommonJS `{ default }` wrapper (for example from an Expo app's root, a package without `"type": "module"`), instead of failing with "permissions.ts defines permissions for unknown table(s): default" or "Invalid schema source". The error says to move the files into a folder with a package.json of `{"type": "module"}`. A real table named `default` keeps working.
