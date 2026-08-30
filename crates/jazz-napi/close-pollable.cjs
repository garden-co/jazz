async function completePollableClose(pending) {
  for (;;) {
    if (pending.poll() !== null) return;
    await new Promise((resolve) => setImmediate(resolve));
  }
}

module.exports = { completePollableClose };
