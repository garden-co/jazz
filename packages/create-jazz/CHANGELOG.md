# create-jazz

## 2.0.0-alpha.47

## 2.0.0-alpha.46

## 2.0.0-alpha.45

### Patch Changes

- 2ee98be: Add sync protocol version checks to the WebSocket handshake so incompatible clients and servers fail with an explicit update prompt.

## 2.0.0-alpha.44

## 2.0.0-alpha.43

### Patch Changes

- 5dec68f: Advance the `create-jazz` spinner to "Provisioning Jazz Cloud app" during the dashboard call, and stop credential/banner output from concatenating onto the active spinner line.

## 2.0.0-alpha.42

## 2.0.0-alpha.41

## 2.0.0-alpha.40

### Patch Changes

- 206f0a9: The "Resolving dependencies" spinner now updates as each package resolves (e.g. `Resolving dependencies (2/5)`), so `npm create jazz` no longer appears frozen during that step.
- b988375: chore: expand scaffold test coverage to all six self-hosted starters

## 2.0.0

### Patch Changes

- c5534e1: Initial release of `create-jazz` — an interactive CLI scaffolder (`npm create jazz`) with six starter templates spanning Next.js and SvelteKit across three auth modes (local-first, hybrid, BetterAuth). Resolves `workspace:*` and `catalog:` dependency references to published versions at scaffold time.
