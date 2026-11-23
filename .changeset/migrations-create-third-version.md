---
"jazz-tools": patch
---

`migrations create` now handles a third schema version ([jazz#6296](https://github.com/garden-co/jazz/issues/6296)). Before, once the app already shipped two versions, editing `tables` again made the earlier migration (written without `to`) target the edited schema, and the command refused with "the migration does not produce the columns". When it moves the previous version into `history`, it now names that version as the `to` of every migration that had none.
