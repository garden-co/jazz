---
name: session
description: Manage git worktree sessions for parallel development. Use for creating, listing, renaming, or retiring sessions.
---

# Git Worktree Session Manager

Manages parallel development sessions using git worktrees. Each session is an isolated working directory with its own branch.

## Commands

Parse the user's request and run the appropriate script:

### List sessions
When user asks to list/show sessions:
```bash
/Users/anselm/jazz2/scripts/session-list
```

### Create new session
When user asks to create/start a new session, they must provide a name:
```bash
/Users/anselm/jazz2/scripts/session-new <session-name>
```
This creates a session with the given name, installs deps, and opens Zed. If the user doesn't provide a name, ask them for one.

### Rename session
When user asks to rename a session (needs old and new name):
```bash
/Users/anselm/jazz2/scripts/session-rename <old-name> <new-name>
```
Renames both the directory and the git branch.

### Retire session
When user asks to retire/remove/delete a session:
```bash
/Users/anselm/jazz2/scripts/session-retire <session-name>
```
Removes the worktree and deletes the branch.

## Examples

- "list sessions" → run session-list
- "create a new session called fix-bug" → run session-new fix-bug
- "start a session for implementing blobs" → run session-new blob-impl
- "rename blob-impl to blob-storage" → run session-rename blob-impl blob-storage
- "retire the old-feature session" → run session-retire old-feature
