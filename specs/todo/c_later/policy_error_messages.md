# Policy Error Messages — TODO (Later)

Manual, UI-ready error messages for policy violations, defined inline in the policy itself.

## Overview

Currently policy denials return generic `PolicyDenied` errors. Instead, policy authors should be able to attach human-readable error messages to specific policy fragments:

```sql
CREATE POLICY edit_own_posts ON posts
  FOR UPDATE
  USING (author_id = current_user_id())
  WITH MESSAGE 'You can only edit your own posts';
```

- Messages defined at the policy level, not the application level
- Returned to the client on denial — ready for direct UI display
- Per-fragment granularity: different messages for different conditions in the same policy

## Open Questions

- i18n — should messages support locale keys, or are they plain strings?
- Multiple policies on same table — which message wins when multiple deny?
- Should messages be part of the schema catalogue or a separate metadata channel?
