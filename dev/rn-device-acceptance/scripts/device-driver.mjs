const PREFIX = "JAZZ_DEVICE_RESULT ";

function parseResult(line) {
  if (!line.startsWith(PREFIX)) return null;
  try { return JSON.parse(line.slice(PREFIX.length)); } catch { return null; }
}

export function collectResults(output) {
  return output.split(/\r?\n/).map(parseResult).filter(Boolean);
}

export function assertDeviceReceipt(output, platform) {
  const results = collectResults(output).filter((item) => item.receipt?.platform === platform);
  if (results.length === 0) throw new Error(`No ${platform} device receipt was emitted by the app`);
  const incomplete = results.filter((item) => item.state !== "passed");
  if (incomplete.length) throw new Error(`${platform} device acceptance is incomplete: ${incomplete.map((item) => `${item.scenario}=${item.state}`).join(", ")}`);
  return results;
}
