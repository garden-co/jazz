---
"jazz-tools": patch
---

A client session takes no `appId`: the app id belongs to your backend's store (`remote({ appId })`, `sqlite({ appId })`), and a browser or app client is scoped by its `serverUrl`. `createJazzSession`, `createJazzApp`, the framework providers (including Vue's `JazzProvider`, which drops its `appId` prop) and the React Native and Expo sessions now refuse one with an error that says where it goes. An app whose local-first users were created under an app id keeps them with `scope: "<that app id>"`; `useLocalFirstAuth` takes `scope` in place of `appId` too. The hero examples read `JAZZ_APP_ID` on the server only, and the examples E2E gate sets that instead of a public `<prefix>JAZZ_APP_ID`.
