const assert = require("node:assert/strict");
const test = require("node:test");
const { completePollableClose } = require("../close-pollable.cjs");

test("a cold native close remains owned until its pollable operation completes", async () => {
  let polls = 0;
  let settled = false;
  const close = completePollableClose({
    poll() {
      polls += 1;
      return polls < 3 ? null : new Uint8Array();
    },
  }).then(() => {
    settled = true;
  });

  await Promise.resolve();
  assert.equal(settled, false);
  await close;
  assert.equal(polls, 3);
  assert.equal(settled, true);
});
