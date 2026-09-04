# INV-HIST-11

- Status: now
- Coverage: ✓

## Invariant

Content and deletion state MUST be separate layers; content writes MUST NOT change the deletion register, and a current `DeletionEvent::Deleted` MUST hide the content-current row until a current `DeletionEvent::Restored` reveals it.

## Enforced by (tests)

`jazz::node::tests::general::deletion_register_hides_and_restore_reveals_current_content`; `jazz::oracle::tests::deletion_register_hides_newer_concurrent_content_until_restore`

## Implementation

`jazz/src/node/codec.rs::current_version_index`; `jazz/src/oracle.rs::visible_current_version`
