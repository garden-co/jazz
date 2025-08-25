---
"jazz-auth-betterauth": minor
"community-jazz-vue": minor
"jazz-tools": minor
"jazz-run": minor
"cojson": minor
"svelte-passkey-auth": patch
"jazz-react-passkey-auth-starter": patch
"chat-svelte": patch
---

Add `$jazz` field to CoValues:
- This field contains Jazz methods that cluttered CoValues' API, as well as Jazz internal properties. This field is not enumerable, to allow CoValues to behave similarly to JSON objects.
- Added a `$jazz.set` method to update a CoValue's fields. When updating collaborative fields, you can pass in JSON objects instead of CoValues and Jazz will create
the CoValues automatically (similarly to CoValue `create` methods).
- All CoMap methods have been moved into `$jazz`, to allow defining any arbitrary key in the CoMap (except for `$jazz`) without conflicts.
  - For CoMaps created with `co.map`, fields are now `readonly` to prevent setting properties directly. Use the `$jazz.set` method instead.
  - CoMaps created with class schemas don't get type errors on direct property assignments, but they get a runtime errors prompting indicating to use `$jazz.set`.
  - the `delete` operator can no longer be used to delete CoRecord properties. Use `$jazz.delete` instead.
- CoList's array-mutation methods have been moved into `$jazz`, in order to prevent using methods 
  - CoLists are now readonly arrays. Trying to use any mutation method yields a type error.
  - `$jazz.set` can be used in place of direct element assignments.
  - Added two new utility methods: `$jazz.remove` and `$jazz.retain`. They allow editing a CoList in-place with a simpler API than `$jazz.splice`.
  - `sort`, `reverse`, `fill` and `copyWithin` have been deprecated, given that they could behave inconsistently with CoLists. `$jazz` replacements may be introduced
  in future releases.
- `.$jazz.owner` now always returns a Group (instead of a Group or an Account). We'll be migrating away of having Accounts as CoValue owners in future releases.
- Removed `castAs`, since it's an inherently unsafe operation that bypassed typechecking and enabled using CoValues in unsupported ways.
- Removed the `id` and `_type` fields from `toJSON()`'s output in Account, CoMap, CoFeed & FileStream, to make CoValues behave more similarly to JSON objects.
- Removed the `root` and `profile` fields from Group.
