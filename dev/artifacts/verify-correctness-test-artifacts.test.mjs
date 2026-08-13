import assert from "node:assert/strict";
import test from "node:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { verifyCorrectnessTestArtifacts } from "./verify-correctness-test-artifacts.mjs";

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "jazz-correctness-artifacts-"));
  for (const dir of [
    "crates/jazz-wasm/pkg",
    "packages/jazz-tools/src/types",
    "packages/jazz-tools/src/runtime/native-runtime",
    "crates/jazz/src",
  ])
    mkdirSync(join(root, dir), { recursive: true });
  // Deliberately leave manifests absent: these tests exercise the bounded ABI
  // checks and prove their planted drift is reported alongside provenance.
  writeFileSync(
    join(root, "packages/jazz-tools/src/types/jazz-wasm.d.ts"),
    `declare module "jazz-wasm" { export class WasmDb {\nconnectUpstream(): any;\nconnectUpstreamWithSession(a: number): any;\nacceptSubscriber(a: Uint8Array, claims: any): any;\n} }`,
  );
  writeFileSync(
    join(root, "crates/jazz-wasm/pkg/jazz_wasm.d.ts"),
    `export class WasmDb {\nconnectUpstream(): any;\nconnectUpstreamWithSession(a: number): any;\nacceptSubscriber(a: Uint8Array, claims: any): any;\n}`,
  );
  writeFileSync(
    join(root, "crates/jazz-wasm/pkg/jazz_wasm.js"),
    `export class WasmDb {\nconnectUpstream() {}\nconnectUpstreamWithSession(a) {}\nacceptSubscriber(a, claims) {}\n}`,
  );
  writeFileSync(
    join(root, "crates/jazz/src/wire.rs"),
    "pub const WIRE_PROTOCOL_VERSION: u16 = 9;\n",
  );
  writeFileSync(
    join(root, "packages/jazz-tools/src/runtime/native-runtime/websocket.ts"),
    "export const WIRE_PROTOCOL_VERSION = 9;\n",
  );
  return root;
}

test("reports a stale generated WASM method even when source declarations are current", () => {
  const root = fixture();
  writeFileSync(
    join(root, "crates/jazz-wasm/pkg/jazz_wasm.js"),
    `export class WasmDb {\nconnectUpstream() {}\nconnectUpstreamWithSession(a) {}\nacceptSubscriber(a) {}\n}`,
  );
  assert.match(
    verifyCorrectnessTestArtifacts(root).join("\n"),
    /acceptSubscriber: consumer=2, d.ts=2, glue=1/,
  );
  rmSync(root, { recursive: true, force: true });
});

test("reports a wire-version mismatch", () => {
  const root = fixture();
  writeFileSync(
    join(root, "packages/jazz-tools/src/runtime/native-runtime/websocket.ts"),
    "export const WIRE_PROTOCOL_VERSION = 8;\n",
  );
  assert.match(
    verifyCorrectnessTestArtifacts(root).join("\n"),
    /wire protocol version mismatch: Rust=9, TS=8/,
  );
  rmSync(root, { recursive: true, force: true });
});
