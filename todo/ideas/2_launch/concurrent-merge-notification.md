# Concurrent Merge Notification

## What

Notify the client when a row has been overwritten by a concurrent write (LWW race). Currently merges happen silently — the user never knows their edit lost.

## Why

In a CRDT/LWW-by-row environment, concurrent writes are expected. But app developers (and sometimes end users) need to know when their data was overwritten — not every merge, but specifically "you lost this one." Without this signal, apps can't surface conflict-awareness UX.

## Who

App developers building collaborative features on Jazz; end users of those apps.

## Rough appetite

unknown

## Notes

- Distinct from sync reliability gaps (writes that never arrive). This is about writes that arrive fine but lose the LWW race.
- The system worked correctly — the user just wasn't told.
- Open question: notification at SDK level (callback/event), at query/subscription level, or both?
