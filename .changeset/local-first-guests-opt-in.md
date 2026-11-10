---
"jazz-tools": minor
---

`createJazz` refuses local-first guests unless your `authenticate` lets them in. Return `acceptLocalFirstAuth(request)` (from `jazz-tools/backend`) for the signed-out requests you want guests to make:

```ts
authenticate: async (request) => {
  const session = await auth.api.getSession({ headers: request.headers });
  return session ? { userId: session.user.id } : acceptLocalFirstAuth(request);
},
```

An app with no sign-in, where every browser is a guest, passes `authenticate: acceptLocalFirstAuth`. A refused guest's sync session fails as an auth failure, and its data stays on the device. An open guest session ends once `authenticate` stops accepting guests, at the next `recheckSeconds` check. A request that `authenticate` names as a user is that user's, never a guest's. `startTestJazz` keeps letting guests in; pass `guests: false` to test the refusal.
