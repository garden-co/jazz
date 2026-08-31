// The Inspector is built in this workspace and bundles this private bridge.
// It intentionally is not a `jazz-tools` package export: installed apps must
// not be able to mint the local-only inspection capability.
export { createInspectorLocalQueryOptions as inspectorLocalQueryOptions } from "../../jazz-tools/src/internal/inspector-query.js";
