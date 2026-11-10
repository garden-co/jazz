---
"jazz-tools": patch
"jazz-rn": patch
---

React Native clients read the rows of a server whose app has schema history. The native relay and its foregrounds now open along the migrations the app ships (`defineApp(schema, { migrations })`), as Node and browser clients do, so they store each column where the server does; before, they opened from the head schema alone and read nothing. Attaching a native session now sends the app (`{ schema, migrations }`), so the native relay ABI is 5: rebuild the native app with this release.
