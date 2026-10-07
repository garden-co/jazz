import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

export const textEditorCases = [10_000, 50_000, 100_000, 259_778].flatMap((edits) => [
  { name: `text_editor_open_browser[${edits}]`, operation: "open" as const, edits },
  { name: `text_editor_reload_browser[${edits}]`, operation: "reload" as const, edits },
  { name: `text_editor_sync_edit_browser[${edits}]`, operation: "edit" as const, edits },
]);

export const textEditorBenchmarks: BenchmarkMetadata[] = textEditorCases.map(
  ({ name, operation, edits }) => ({
    name,
    harness: "vitest",
    title: `Text editor · ${operation === "open" ? "open in a fresh browser context" : operation === "reload" ? "reload locally while offline" : "sync an edit to another browser"} (${edits.toLocaleString("en-US")} edits)`,
    description:
      operation === "edit"
        ? "Insert one character at the end of a loaded document and wait until a second browser's CodeMirror view contains it."
        : `${operation === "open" ? "Navigate from a fresh browser context" : "Reload a previously loaded document with WebSockets blocked"} until CodeMirror contains the expected text and is editable. Uses the actual example, which replays Yjs updates before attaching CodeMirror.`,
    fixture: `One document with one binary log containing the first ${edits.toLocaleString("en-US")} individual updates of the checksum-pinned Automerge paper trace. Yjs client ID 1; default garbage collection enabled; no consolidation.`,
    storage:
      "Chromium persistent browser worker and a localhost in-memory Jazz server; Vite development app; inspector disabled",
    includes:
      operation === "edit"
        ? [
            "Playwright text insertion",
            "Yjs encoding and Jazz log append",
            "Sync to a separate browser context and updating its editor",
          ]
        : [
            "Page navigation and application initialization",
            "Jazz subscription delivery",
            "Yjs replay and CodeMirror initialization",
            "Playwright observation of the loaded editor",
          ],
    excludes: [
      "Native/package builds, server startup, fixture generation and seeding",
      "Browser/context creation",
      operation === "edit"
        ? "Initial document loading"
        : "Final full-text correctness assertion and context teardown",
    ],
    work: {
      count: 1,
      unit: operation === "edit" ? "synced edits/s" : "document opens/s",
      explanation:
        "One complete user operation per iteration; historical edits are fixture size, not a throughput denominator.",
    },
    source: "examples/text-editor/benchmarks/editor.bench.mjs",
  }),
);
