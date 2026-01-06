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
When user asks to create/start a new session:
```bash
/Users/anselm/jazz2/scripts/session-new
```
This creates a timestamped session, installs deps, and opens Zed.

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
- "create a new session" → run session-new
- "rename session-20260106 to fix-bug" → run session-rename session-20260106 fix-bug
- "retire the old-feature session" → run session-retire old-feature
