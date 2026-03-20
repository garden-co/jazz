---
"jazz-tools": patch
---

Improve React Native runtime error reporting by normalizing UniFFI bridge failures into standard `Error` objects with stable `name`, `message`, `cause`, and `tag` metadata.

Thanks [Schniz](https://github.com/Schniz)!
