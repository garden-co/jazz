import assert from "node:assert/strict";
import test from "node:test";

import { rcVerdict, summary } from "./rc-verdict.mjs";

const ok = (...jobs) => Object.fromEntries(jobs.map((job) => [job, { result: "success" }]));

test("GO when every leg passed and nothing holds", () => {
  const result = rcVerdict({
    needs: ok("resolve", "examples-e2e", "cloud-rc-probe"),
    probe: "green",
  });
  assert.equal(result.verdict, "GO");
  assert.deepEqual(result.holds, []);
});

test("NO-GO when any leg failed, holds or not", () => {
  const needs = { ...ok("resolve"), "examples-e2e": { result: "failure" } };
  assert.equal(rcVerdict({ needs, probe: "needs-infra" }).verdict, "NO-GO");
});

test("HOLD names each hold and its owner, and is not a failure", () => {
  const result = rcVerdict({
    needs: ok("resolve", "cloud-rc-probe", "adopter-verdict"),
    probe: "needs-infra",
    adopter: "NEEDS_REVIEW",
    adopterAttention: ["sizes: jazz-wasm +3.2%"],
  });
  assert.equal(result.verdict, "HOLD");
  assert.deepEqual(
    result.holds.map((hold) => [hold.name, hold.owner]),
    [
      ["Cloud RC probe: needs-infra", "#infra"],
      ["Adopter gate: NEEDS_REVIEW", "Anselm"],
    ],
  );
  const text = summary(result);
  assert.match(text, /^## Release candidate: HOLD\n\nHOLD: /);
  assert.match(text, /sizes: jazz-wasm \+3\.2%/);
});

test("a dry run may skip publishing, a real cut may not", () => {
  const needs = {
    ...ok("resolve"),
    publish: { result: "skipped" },
    "attach-server": { result: "skipped" },
  };
  assert.equal(rcVerdict({ needs, dryRun: true }).verdict, "GO");
  assert.equal(rcVerdict({ needs, dryRun: false }).verdict, "NO-GO");
});

test("a probe-only run counts only the probe", () => {
  const needs = { ...ok("resolve", "cloud-rc-probe"), "examples-e2e": { result: "skipped" } };
  assert.equal(rcVerdict({ needs, probeOnly: true, probe: "green" }).verdict, "GO");
});

test("of alternative jobs exactly one must run and pass", () => {
  const alternatives = [["rn-device-acceptance", "rn-device-acceptance-built"]];
  const with_ = (fast, built) => ({
    ...ok("resolve"),
    "rn-device-acceptance": { result: fast },
    "rn-device-acceptance-built": { result: built },
  });
  assert.equal(rcVerdict({ needs: with_("success", "skipped"), alternatives }).verdict, "GO");
  assert.equal(rcVerdict({ needs: with_("skipped", "success"), alternatives }).verdict, "GO");
  assert.equal(rcVerdict({ needs: with_("skipped", "skipped"), alternatives }).verdict, "NO-GO");
  assert.equal(rcVerdict({ needs: with_("failure", "skipped"), alternatives }).verdict, "NO-GO");
  assert.equal(rcVerdict({ needs: with_("skipped", "failure"), alternatives }).verdict, "NO-GO");
  assert.equal(rcVerdict({ needs: with_("success", "skipped") }).verdict, "NO-GO");
});
