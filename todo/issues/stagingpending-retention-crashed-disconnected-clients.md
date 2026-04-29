# StagingPending Retention With Crashed Or Disconnected Clients

## What

Server-side `StagingPending` transactional rows may need an explicit retention and cleanup policy when the originating client crashes or stays disconnected before commit or rollback.

## Priority

unknown

## Notes

This came up while adding rollback `CancelBatch`: explicit rollback can discard pending rows, but crashes or long disconnections can leave pending transactional rows without a cancel or seal.
