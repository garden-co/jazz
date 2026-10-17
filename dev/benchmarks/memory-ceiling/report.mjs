// Renders a memory-ceiling run (summary.json and steps.json) as Markdown.
const MiB = 1024 * 1024;
const GiB = 1024 * MiB;

export const fmt = (bytes) =>
  Math.abs(bytes) >= GiB ? `${(bytes / GiB).toFixed(2)} GiB` : `${(bytes / MiB).toFixed(0)} MiB`;

function renderCounts(counts, unit) {
  return Object.entries(counts)
    .filter(([k]) => k !== unit)
    .map(([k, v]) => `${v.toLocaleString("en-US")} ${k}`)
    .join(", ");
}

/** The run as Markdown, for the job summary and the README. */
export function renderSummary(summary, steps) {
  const lines = [];
  const sideNames = {
    serverImport: "server, while importing",
    serverEdit: "server, a user's policy-checked edit",
    serverCold: "server, restart and users reconnecting",
    client: "client, opening the app",
  };
  lines.push(`### ${summary.app}: ${summary.scenario} (${summary.unit})`, "");
  lines.push(
    `jazz-tools ${summary.jazzTools}; budgets: server ${fmt(summary.budgets.server)}, client ${fmt(summary.budgets.client)} of growth; growth writes: ${summary.writes}.`,
    "",
  );
  if (summary.firstToCross) {
    const c = summary.ceilings[summary.firstToCross];
    lines.push(
      `**Ceiling: ${c.units.toLocaleString("en-US")} ${summary.unit}** (${renderCounts(c.counts, summary.unit)}); first to cross: ${sideNames[summary.firstToCross]}, over budget at ${c.overAt.toLocaleString("en-US")} ${summary.unit}.`,
      "",
    );
    for (const [side, other] of Object.entries(summary.ceilings)) {
      if (side !== summary.firstToCross)
        lines.push(
          `- Also crossed: ${sideNames[side]} at ${other.overAt.toLocaleString("en-US")} ${summary.unit}.`,
        );
    }
  } else {
    lines.push(`**No budget crossed** (${summary.reached}).`, "");
  }
  const per = summary.bytesPerUnit;
  const perLine = (label, v) =>
    v == null
      ? null
      : `- ${label}: ${(v / 1024).toFixed(1)} KiB per ${summary.unit.replace(/s$/, "")}`;
  lines.push(
    "",
    "Growth per unit (least squares over the steps):",
    ...[
      perLine("server peak while importing", per.serverImportPeak),
      perLine("server peak during a user's edits", per.serverEditPeak),
      perLine("server peak, restart + users", per.serverColdPeak),
      perLine("server idle after restart", per.serverIdleAfterRestart),
      perLine("client growth opening the app", per.clientPeak),
      perLine("server data on disk", per.disk),
    ].filter(Boolean),
  );
  const last = summary.lastStep;
  if (last?.churn) {
    lines.push(
      "",
      `Session churn at ${last.scale.toLocaleString("en-US")} ${summary.unit}: each time the same user opens the app and leaves, the server keeps ${fmt(last.churn.rssPerSession)} more RSS (${fmt(last.churn.heapPerSession ?? 0)} sampled heap)` +
        (last.sessionsToBudget != null
          ? `; ~${last.sessionsToBudget} such sessions from idle reach the budget.`
          : "."),
    );
  }
  if (last?.probe?.msPerTransaction != null) {
    lines.push(
      `A user's policy-checked edit at that size: ${last.probe.msPerTransaction} ms (${last.probe.serverCpuMsPerTransaction} ms server CPU).`,
    );
  }
  lines.push(
    "",
    `| ${summary.unit} | rows | import peak | edits peak | edit ms | restart idle | one user | + crowd | after disconnect | client growth |`,
    "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
  );
  for (const s of steps) {
    const c = s.cold ?? {};
    const cell = (v) => (v ? fmt(v) : "–");
    lines.push(
      `| ${s.scale.toLocaleString("en-US")} | ${s.ingest.rows.toLocaleString("en-US")} | ${cell(Math.max(0, ...s.ingest.chunks.map((ch) => ch.serverPeak)))} | ${cell(s.ingest.probe?.serverPeak)} | ${s.ingest.probe?.msPerTransaction ?? (s.ingest.probe?.error ? "rejected" : "–")} | ${cell(c.serverIdle)} | ${cell(c.serverPeakClientOnly)} | ${cell(c.serverPeak && Math.max(c.serverPeak, c.serverHwm))} | ${cell(c.serverAfterDisconnect)} | ${cell(c.clientGrowth)} |`,
    );
  }
  const sites = (title, heap) => {
    if (!heap) return;
    lines.push(
      "",
      `Top server allocation sites ${title} at ${last.scale.toLocaleString("en-US")} ${summary.unit} (${fmt(heap.inUseBytes)} sampled in use):`,
      "",
    );
    for (const site of heap.sites.slice(0, 8))
      lines.push(`- ${fmt(site.bytes)} (${(site.share * 100).toFixed(0)}%) \`${site.name}\``);
  };
  sites("after the import and a user's edits", last?.heapIngest);
  sites("with the restarted server's users connected", last?.heapCold);
  return lines.join("\n") + "\n";
}
