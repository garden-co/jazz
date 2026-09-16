"use client";

import { useEffect, useMemo, useState } from "react";
import {
  getBenchmarkMetadata,
  metadataRevision,
  displayedTime,
  formatThroughput,
  ESTIMATE_DIVISOR,
} from "@/lib/perf-timeline/presentation";
import {
  checkpoint,
  calendarDay,
  plotGeometry,
  formatTime,
  stages,
  type Benchmark,
  type Point,
  type Stage,
  type Timeline,
} from "@/lib/perf-timeline/model";

const repo = "https://github.com/garden-co/jazz";
const codspeed = "https://app.codspeed.io/garden-co/jazz";
const priority = [
  "first_sync_27518_rocksdb",
  "sequential_insert_1350_rocksdb",
  "sequential_update_1350_rocksdb",
];
const pretty = (name: string) => name.replaceAll("_", " ");
const date = (value: string) =>
  new Date(value).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  });

function StageLabel({ stage }: { stage: Stage }) {
  return (
    <span className={`stage ${stage}`}>
      <svg width="24" height="10" aria-hidden="true">
        <line
          x1="0"
          x2="24"
          y1="5"
          y2="5"
          stroke={stages[stage].color}
          strokeWidth={stage === "released" ? 4 : 2}
          strokeDasharray={stages[stage].dash}
        />
      </svg>
      {stages[stage].label}
    </span>
  );
}

function Sparkline({
  points,
  logarithmic,
  spread,
  estimated,
}: {
  points: Point[];
  logarithmic: boolean;
  spread: boolean;
  estimated: boolean;
}) {
  if (!points.length) return null;
  const geometry = plotGeometry(points, logarithmic, spread, estimated ? ESTIMATE_DIVISOR : 1);
  const prior = new Map<string, number>();
  return (
    <svg className="sparkline" viewBox="0 0 70 24" aria-hidden="true">
      {points.map((p, i) => {
        const previous = prior.get(p.series);
        prior.set(p.series, i);
        return (
          <g key={p.resultId}>
            {previous !== undefined && (
              <line
                x1={3 + geometry.x(previous) * 64}
                x2={3 + geometry.x(i) * 64}
                y1={3 + geometry.y(points[previous].median) * 18}
                y2={3 + geometry.y(p.median) * 18}
                stroke={stages[p.stage].color}
                strokeWidth="0.8"
                strokeDasharray={p.stage === "open" ? "2 2" : undefined}
              />
            )}
            <circle
              cx={3 + geometry.x(i) * 64}
              cy={3 + geometry.y(p.median) * 18}
              r="1"
              fill={stages[p.stage].color}
            />
          </g>
        );
      })}
    </svg>
  );
}

function Chart({
  points,
  selected,
  onSelect,
  logarithmic,
  spread,
  estimated,
}: {
  points: Point[];
  selected: string | null;
  onSelect: (id: string) => void;
  logarithmic: boolean;
  spread: boolean;
  estimated: boolean;
}) {
  const width = 1100,
    height = 395,
    left = 78,
    right = 78,
    top = 25,
    bottom = 105;
  const geometry = plotGeometry(points, logarithmic, spread, estimated ? ESTIMATE_DIVISOR : 1);
  const y = (v: number) => top + geometry.y(v) * (height - top - bottom);
  const x = (i: number) => left + geometry.x(i) * (width - left - right);
  const groups = new Map<string, number[]>();
  points.forEach((p, i) => groups.set(p.series, [...(groups.get(p.series) ?? []), i]));
  const tickStep = Math.max(1, Math.ceil(points.length / 8));
  return (
    <div className="chart-scroll">
      <svg
        className="chart"
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label={`${estimated ? "Estimated wallclock (deterministic runner divided by five)" : "Measured deterministic runner wallclock"} by chronological checkpoint. Use the receipt table below for exact measured values.`}
      >
        {geometry.ticks.map((value, i) => {
          return (
            <g key={i}>
              <line
                x1={left}
                x2={width - right}
                y1={y(value)}
                y2={y(value)}
                stroke="#e6e5de"
                strokeDasharray="3 5"
              />
              <text
                x={left - 14}
                y={y(value) + 4}
                textAnchor="end"
                className="axis-text y-axis-left"
              >
                {displayedTime(value, estimated)}
              </text>
              <text
                x={width - right + 14}
                y={y(value) + 4}
                textAnchor="start"
                className="axis-text y-axis-right"
              >
                {displayedTime(value, estimated)}
              </text>
            </g>
          );
        })}
        {[...groups.values()].flatMap((indexes) =>
          indexes.slice(1).map((index, i) => {
            const previous = indexes[i];
            const style = stages[points[index].stage];
            return (
              <line
                key={`${previous}-${index}`}
                x1={x(previous)}
                y1={y(points[previous].median)}
                x2={x(index)}
                y2={y(points[index].median)}
                stroke={style.color}
                strokeWidth={points[index].stage === "released" ? 3.5 : 2}
                strokeDasharray={style.dash}
                opacity="0.8"
              />
            );
          }),
        )}
        {points.map((p, i) => (
          <g key={p.resultId}>
            {spread && (
              <line
                x1={x(i)}
                x2={x(i)}
                y1={y(p.min)}
                y2={y(p.max)}
                stroke={stages[p.stage].color}
                strokeWidth="5"
                opacity="0.2"
              />
            )}
            {selected === p.resultId && (
              <line
                x1={x(i)}
                x2={x(i)}
                y1={top}
                y2={height - bottom}
                stroke={stages[p.stage].color}
                opacity="0.25"
              />
            )}
            <circle
              className="chart-point"
              cx={x(i)}
              cy={y(p.median)}
              r={selected === p.resultId ? 6 : p.stage === "released" ? 5 : 3.5}
              fill={p.stage === "open" ? "#fafaf7" : stages[p.stage].color}
              stroke={stages[p.stage].color}
              strokeWidth="2"
              onClick={() => onSelect(p.resultId)}
              tabIndex={0}
              role="button"
              aria-label={`${checkpoint(p)}, ${displayedTime(p.median, estimated)}, ${stages[p.stage].label}`}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onSelect(p.resultId);
                }
              }}
            >
              <title>
                {checkpoint(p)} · {displayedTime(p.median, estimated)} · {date(p.date)}
                {`\n`}
                {p.title}
              </title>
            </circle>
            {((i % tickStep === 0 && points.length - 1 - i >= tickStep / 2) ||
              i === points.length - 1) && (
              <g>
                <text x={x(i)} y={height - bottom + 28} textAnchor="middle" className="axis-text">
                  {calendarDay(p.date)}
                </text>
                <text x={x(i)} y={height - bottom + 46} textAnchor="middle" className="axis-text">
                  {p.backfill?.releaseTag ??
                    p.release ??
                    (p.pr ? `PR #${p.pr}` : p.stage === "main" ? "main commit" : "commit")}
                </text>
                <text x={x(i)} y={height - bottom + 63} textAnchor="middle" className="axis-sha">
                  {p.sha.slice(0, 7)}
                </text>
              </g>
            )}
          </g>
        ))}
        <text x={left} y="12" className="axis-caption">
          {estimated ? "ESTIMATED WALLCLOCK*" : "DETERMINISTIC RUNNER WALLCLOCK"} ·{" "}
          {logarithmic ? "LOG SCALE" : "ZERO-BASED SCALE"}
        </text>
        <text x={width - right} y={height - 6} textAnchor="end" className="axis-caption">
          MEASURED CHECKPOINTS · CHECKPOINT DAY (UTC) →
        </text>
      </svg>
    </div>
  );
}

export function Dashboard() {
  const [data, setData] = useState<Timeline | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [benchmarkId, setBenchmarkId] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [days, setDays] = useState("all");
  const [stage, setStage] = useState("all");
  const [branch, setBranch] = useState("all");
  const [logarithmic, setLogarithmic] = useState(true);
  const [spread, setSpread] = useState(false);
  const [estimated, setEstimated] = useState(true);

  async function refresh() {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch("/api/timeline");
      if (!response.ok)
        throw new Error("CodSpeed history is temporarily unavailable. Please try again.");
      const incoming: Timeline = await response.json();
      setData(incoming);
      const requested = new URLSearchParams(window.location.search).get("benchmark");
      setBenchmarkId(
        (current) =>
          incoming.benchmarks.find((b) => b.id === current)?.id ??
          incoming.benchmarks.find((b) => b.id === requested || b.name === requested)?.id ??
          incoming.benchmarks.find((b) => b.name === priority[0])?.id ??
          incoming.benchmarks[0]?.id ??
          null,
      );
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Unable to load history.");
    } finally {
      setLoading(false);
    }
  }
  useEffect(() => {
    void refresh();
  }, []);
  const benchmarks = useMemo(
    () =>
      [...(data?.benchmarks ?? [])].sort((a, b) => {
        const aRank = priority.indexOf(a.name),
          bRank = priority.indexOf(b.name);
        return (aRank < 0 ? 99 : aRank) - (bRank < 0 ? 99 : bRank) || a.name.localeCompare(b.name);
      }),
    [data],
  );
  const benchmark: Benchmark | undefined = benchmarks.find((b) => b.id === benchmarkId);
  const metadata = benchmark ? getBenchmarkMetadata(benchmark.name) : null;
  const points = useMemo(() => {
    const cutoff = days === "all" ? 0 : Date.now() - Number(days) * 86400000;
    return (benchmark?.points ?? []).filter(
      (p) =>
        Date.parse(p.date) >= cutoff &&
        (stage === "all" || p.stage === stage) &&
        (branch === "all" || p.series === branch),
    );
  }, [benchmark, days, stage, branch]);
  const current = points.find((p) => p.resultId === selected) ?? points.at(-1);
  const series = [
    ...new Map(
      (benchmark?.points ?? []).map((p) => [
        p.series,
        p.series === "main" ? "main" : p.pr ? `PR #${p.pr} · ${p.branch}` : p.branch,
      ]),
    ).entries(),
  ];
  const first = points[0],
    latest = points.at(-1);
  const matched = benchmarks.filter((b) => b.name.toLowerCase().includes(search.toLowerCase()));
  function selectBenchmark(id: string) {
    setBenchmarkId(id);
    setSelected(null);
    setBranch("all");
    const url = new URL(window.location.href);
    url.searchParams.set("benchmark", id);
    window.history.replaceState(null, "", url);
  }
  return (
    <div className="perf-timeline">
      <header className="topbar">
        <span className="brand-caption">Performance lab</span>
        <nav>
          <a href={`${repo}/issues/2913`} target="_blank" rel="noreferrer">
            Research log ↗
          </a>
          <a href={codspeed} target="_blank" rel="noreferrer">
            CodSpeed ↗
          </a>
        </nav>
      </header>
      <div className="page-shell">
        <section className="intro">
          <div>
            <div className="eyebrow">
              <span className="status-dot" /> ENGINEERING / MEASUREMENTS
            </div>
            <h1>A little faster, every commit.</h1>
            <p>Wallclock performance across releases, main, and work in progress.</p>
          </div>
          <button className="refresh" onClick={() => void refresh()} disabled={loading}>
            {loading ? "Fetching…" : "↻ Refresh data"}
          </button>
        </section>
        <div className="workspace">
          <aside className="sidebar">
            <div className="sidebar-heading">
              Benchmarks <span>{benchmarks.length || "—"}</span>
            </div>
            <label className="search">
              <span aria-hidden="true">⌕</span>
              <input
                placeholder="Find a benchmark…"
                aria-label="Find a benchmark"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
            </label>
            <div className="bench-list">
              {loading && !data ? (
                <div className="empty-small">Reading CodSpeed history…</div>
              ) : (
                matched.map((b) => (
                  <button
                    className={`bench-item ${benchmarkId === b.id ? "active" : ""}`}
                    key={b.id}
                    onClick={() => selectBenchmark(b.id)}
                  >
                    <span className="bench-name">{pretty(b.name)}</span>
                    <span className="bench-meta">
                      <span>{b.points.length} measurements</span>
                      <Sparkline
                        points={
                          b.id === benchmarkId
                            ? points
                            : b.points.filter(
                                (p) =>
                                  (days === "all" ||
                                    Date.parse(p.date) >= Date.now() - Number(days) * 86400000) &&
                                  (stage === "all" || p.stage === stage),
                              )
                        }
                        logarithmic={logarithmic}
                        spread={spread}
                        estimated={estimated}
                      />
                    </span>
                  </button>
                ))
              )}
              {!loading && matched.length === 0 && (
                <div className="empty-small">No wallclock benchmarks found.</div>
              )}
            </div>
            <div className="sidebar-foot">
              Public data · garden-co/jazz
              <br />
              Wallclock only, never simulated time.
            </div>
          </aside>
          <main>
            {error && (
              <div className="notice error" role="alert">
                {error} <button onClick={() => void refresh()}>Retry</button>
                {data && " Showing the previously loaded data."}
              </div>
            )}
            {data?.warnings.map((warning) => (
              <div className="notice" role="status" key={warning}>
                {warning}
              </div>
            ))}
            {!data && (
              <div className="loading-panel">
                <div className="loading-orbit" />
                <h2>{error ? "History is taking a break." : "Gathering the receipts."}</h2>
                <p>
                  {error
                    ? "Retry to reconnect to the benchmark source."
                    : "Fetching actual run medians and source checkpoints from CodSpeed."}
                </p>
              </div>
            )}
            {data && benchmark && (
              <>
                <div className="benchmark-heading">
                  <div>
                    <div className="eyebrow">
                      BENCHMARK /{" "}
                      {benchmark.name.includes("rocksdb")
                        ? "ROCKSDB"
                        : benchmark.name.includes("memory")
                          ? "IN MEMORY"
                          : "NATIVE WALLCLOCK"}
                    </div>
                    <h2>{metadata?.title ?? pretty(benchmark.name)}</h2>
                    <code>{benchmark.name}</code>
                  </div>
                  <a
                    className="icon-link"
                    href={`${codspeed}/benchmarks/${benchmark.id}`}
                    target="_blank"
                    rel="noreferrer"
                    aria-label="Open benchmark in CodSpeed"
                  >
                    ↗
                  </a>
                </div>
                {metadata ? (
                  <section className="benchmark-description" aria-label="Benchmark description">
                    <p>{metadata.description}</p>
                    <div className="definition-context">
                      <span>{metadata.storage}</span>
                      <span>{metadata.fixture}</span>
                    </div>
                    <details>
                      <summary>What is timed & how throughput is counted</summary>
                      <div className="timing-boundaries">
                        <div>
                          <h4>Inside the timer</h4>
                          <ul>
                            {metadata.includes.map((text) => (
                              <li key={text}>{text}</li>
                            ))}
                          </ul>
                        </div>
                        <div>
                          <h4>Outside the timer</h4>
                          <ul>
                            {metadata.excludes.map((text) => (
                              <li key={text}>{text}</li>
                            ))}
                          </ul>
                        </div>
                      </div>
                      <p className="work-unit">
                        {metadata.work.explanation} Rate ={" "}
                        {metadata.work.count.toLocaleString("en-US")} ÷ timed seconds.
                      </p>
                      <p className="definition-source">
                        Reviewed definition v{metadataRevision.version} ·{" "}
                        <a
                          href={`${repo}/blob/${metadataRevision.reviewedCommit}/${metadata.source}`}
                          target="_blank"
                          rel="noreferrer"
                        >
                          Reviewed harness ↗
                        </a>
                        {current && (
                          <>
                            {" "}
                            ·{" "}
                            <a
                              href={`${repo}/blob/${current.sha}/${metadata.source}`}
                              target="_blank"
                              rel="noreferrer"
                            >
                              Harness at selected commit ↗
                            </a>
                          </>
                        )}
                        <br />
                        Historical timer boundaries may differ; this description is not a claim that
                        every run used the same harness revision.
                      </p>
                    </details>
                  </section>
                ) : (
                  <p className="benchmark-description">
                    No reviewed description or throughput denominator is registered for this
                    benchmark yet. Timings remain available.
                  </p>
                )}
                <div className="metrics">
                  <div>
                    <span>First shown</span>
                    <strong>{first ? displayedTime(first.median, estimated) : "—"}</strong>
                    {first && metadata && (
                      <small className="metric-throughput">
                        {formatThroughput(first.median, metadata, estimated)}
                      </small>
                    )}
                    {first && (
                      <small className="estimate">
                        {estimated
                          ? `Deterministic runner: ${formatTime(first.median)}`
                          : `Estimated: ${displayedTime(first.median, true)}`}
                      </small>
                    )}
                    <small>{first ? checkpoint(first) : "Adjust the filters"}</small>
                  </div>
                  <div>
                    <span>Latest shown</span>
                    <strong>{latest ? displayedTime(latest.median, estimated) : "—"}</strong>
                    {latest && metadata && (
                      <small className="metric-throughput">
                        {formatThroughput(latest.median, metadata, estimated)}
                      </small>
                    )}
                    {latest && (
                      <small className="estimate">
                        {estimated
                          ? `Deterministic runner: ${formatTime(latest.median)}`
                          : `Estimated: ${displayedTime(latest.median, true)}`}
                      </small>
                    )}
                    <small>{latest ? checkpoint(latest) : "No measurements"}</small>
                  </div>
                  <div>
                    <span>Measured checkpoints</span>
                    <strong>
                      {points.length}
                      <em> / {benchmark.points.length}</em>
                    </strong>
                    <small>Median per run · lower is better</small>
                  </div>
                </div>
                <section className="plot-panel">
                  <div className="plot-toolbar">
                    <h3>Wallclock timeline</h3>
                    <div className="plot-options">
                      <label>
                        <input
                          type="checkbox"
                          checked={spread}
                          onChange={(e) => setSpread(e.target.checked)}
                        />{" "}
                        Min–max
                      </label>
                      <label>
                        <input
                          type="checkbox"
                          checked={logarithmic}
                          onChange={(e) => setLogarithmic(e.target.checked)}
                        />{" "}
                        Log scale
                      </label>
                    </div>
                  </div>
                  <div className="filters">
                    <label>
                      Timing
                      <select
                        aria-label="Timing display"
                        value={estimated ? "estimated" : "measured"}
                        onChange={(event) => setEstimated(event.target.value === "estimated")}
                      >
                        <option value="estimated">Estimated machine*</option>
                        <option value="measured">Deterministic runner</option>
                      </select>
                    </label>
                    <label>
                      Window
                      <select
                        aria-label="Time window"
                        value={days}
                        onChange={(e) => setDays(e.target.value)}
                      >
                        <option value="all">All available history</option>
                        <option value="7">Last 7 days</option>
                        <option value="30">Last 30 days</option>
                      </select>
                    </label>
                    <label>
                      Status
                      <select
                        aria-label="Checkpoint status"
                        value={stage}
                        onChange={(e) => setStage(e.target.value)}
                      >
                        <option value="all">All checkpoints</option>
                        {Object.entries(stages).map(([key, value]) => (
                          <option key={key} value={key}>
                            {value.label}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label className="branch-filter">
                      Trace
                      <select
                        aria-label="Branch trace"
                        value={branch}
                        onChange={(e) => setBranch(e.target.value)}
                      >
                        <option value="all">All branches / PRs</option>
                        {series.map(([id, label]) => (
                          <option key={id} value={id}>
                            {label}
                          </option>
                        ))}
                      </select>
                    </label>
                  </div>
                  {points.length ? (
                    <Chart
                      points={points}
                      selected={current?.resultId ?? null}
                      onSelect={setSelected}
                      logarithmic={logarithmic}
                      spread={spread}
                      estimated={estimated}
                    />
                  ) : (
                    <div className="empty-chart">
                      No measurements in this selection. Try a wider window or another status.
                    </div>
                  )}
                  <div className="legend">
                    {Object.keys(stages).map((s) => (
                      <StageLabel stage={s as Stage} key={s} />
                    ))}
                  </div>
                  <p className="chart-note">
                    {estimated && (
                      <>
                        * Estimated time = deterministic runner ÷ 5, not a measured hardware
                        result.{" "}
                      </>
                    )}
                    Each line follows one branch or PR, ordered by checkpoint date. Dates are UTC.
                    Historical backfills use release publication dates; receipts retain actual
                    measurement dates. Min–max is sample range, not a confidence interval.
                  </p>
                </section>
                {current && (
                  <section className="receipt">
                    <div className="receipt-top">
                      <div className="eyebrow">SELECTED RECEIPT</div>
                      <StageLabel stage={current.stage} />
                    </div>
                    <div className="receipt-body">
                      <div>
                        <h3>{current.title.split("\n")[0]}</h3>
                        <p>
                          {current.backfill
                            ? `Release date ${date(current.date)} · measured ${date(current.measuredAt)}`
                            : date(current.date)}{" "}
                          · {current.branch}
                        </p>
                        <div className="receipt-links">
                          <a
                            href={`${repo}/commit/${current.sha}`}
                            target="_blank"
                            rel="noreferrer"
                          >
                            {current.sha.slice(0, 10)} ↗
                          </a>
                          {current.backfill && (
                            <a
                              href={`${repo}/commit/${current.backfill.engineSha}`}
                              target="_blank"
                              rel="noreferrer"
                            >
                              Released engine {current.backfill.engineSha.slice(0, 10)} ↗
                            </a>
                          )}
                          {current.pr && (
                            <a href={`${repo}/pull/${current.pr}`} target="_blank" rel="noreferrer">
                              PR #{current.pr} ({current.prStatus?.toLowerCase()}) ↗
                            </a>
                          )}
                          <a
                            href={`${codspeed}/runs/${current.runId}`}
                            target="_blank"
                            rel="noreferrer"
                          >
                            CodSpeed run ↗
                          </a>
                        </div>
                      </div>
                      <div className="receipt-timing">
                        <strong>{displayedTime(current.median, estimated)}</strong>
                        {metadata && (
                          <span className="metric-throughput">
                            {formatThroughput(current.median, metadata, estimated)}
                          </span>
                        )}
                        <span>
                          {displayedTime(current.min, estimated)} –{" "}
                          {displayedTime(current.max, estimated)}
                        </span>
                        <small>
                          {estimated
                            ? "estimated median · scaled min–max*"
                            : "Deterministic runner median · observed min–max"}
                        </small>
                        <small>
                          {estimated
                            ? `Deterministic runner: ${formatTime(current.median)}`
                            : `Estimated: ${displayedTime(current.median, true)}`}
                        </small>
                      </div>
                    </div>
                    {metadata && (
                      <div className="throughput-receipt" aria-label="Throughput receipt">
                        <div>
                          <span>Deterministic runner workload rate</span>
                          <strong>{formatThroughput(current.median, metadata)}</strong>
                        </div>
                        <div>
                          <span>Estimated workload rate*</span>
                          <strong>{formatThroughput(current.median, metadata, true)}</strong>
                        </div>
                        <p>{metadata.work.explanation}</p>
                      </div>
                    )}
                    <div className="receipt-id">
                      {current.backfill && (
                        <span>
                          Historical harness {current.sha.slice(0, 10)} ·{" "}
                          {current.backfill.dateSource} ·{" "}
                          <a href={current.backfill.workflowUrl} target="_blank" rel="noreferrer">
                            Provenance artifact ↗
                          </a>{" "}
                          ·{" "}
                        </span>
                      )}
                      {current.includedInRelease && (
                        <span>
                          Included in {current.includedInRelease}
                          {current.release
                            ? " (exact release commit)"
                            : " (ancestor; this timing is for the measured commit)"}{" "}
                          ·{" "}
                        </span>
                      )}
                      Result {current.resultId} · run {current.runStatus.toLowerCase()}
                      {current.runStatus !== "COMPLETED" &&
                        " · other jobs may be incomplete or failed"}
                    </div>
                  </section>
                )}
                <details className="receipts-table">
                  <summary>
                    All {points.length} measurement receipts{" "}
                    <span>Exact values & source links</span>
                  </summary>
                  <div className="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Checkpoint</th>
                          <th>Status</th>
                          <th>Measured</th>
                          <th>Deterministic runner median</th>
                          <th>Measured min–max</th>
                          <th>Estimated time*</th>
                          {metadata && <th>Measured workload rate</th>}
                          <th>Source</th>
                        </tr>
                      </thead>
                      <tbody>
                        {points.toReversed().map((p) => (
                          <tr
                            key={p.resultId}
                            className={current?.resultId === p.resultId ? "selected-row" : ""}
                          >
                            <td>
                              <button onClick={() => setSelected(p.resultId)}>
                                {checkpoint(p)}
                              </button>
                            </td>
                            <td>
                              <StageLabel stage={p.stage} />
                            </td>
                            <td>
                              {date(p.date)}
                              {p.backfill && <small> · measured {date(p.measuredAt)}</small>}
                            </td>
                            <td className="numeric">{p.median.toFixed(9)} s</td>
                            <td className="numeric">
                              {formatTime(p.min)} – {formatTime(p.max)}
                            </td>
                            <td className="numeric">{displayedTime(p.median, true)}</td>
                            {metadata && (
                              <td className="numeric">{formatThroughput(p.median, metadata)}</td>
                            )}
                            <td>
                              <a
                                href={`${codspeed}/runs/${p.runId}`}
                                target="_blank"
                                rel="noreferrer"
                              >
                                Run ↗
                              </a>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>
              </>
            )}
            {data && (
              <section className="methodology">
                <h3>Read the graph, keep the context.</h3>
                <p id="estimate-footnote" className="estimate-footnote">
                  <strong>* Rough machine estimate:</strong> estimated time = deterministic runner
                  time ÷ 5; estimated throughput = measured workload rate × 5. This is a simple
                  illustrative assumption, not a measurement or hardware-specific calibration.
                  Actual machines, storage, build settings and workloads can differ substantially.
                  Measured runner values remain unchanged in the receipts.
                </p>
                <p>
                  “Deterministic runner” labels our CodSpeed runner measurements; wallclock samples
                  still vary. Measured values are benchmark-operation medians, not per-row latency
                  or local-machine timings. Throughput is documented work units divided by the
                  median duration, not a separately measured mean or a concurrent-capacity claim.
                  Different harness revisions, fixtures, and runner configurations can change
                  comparability; follow the commit and run receipts before making a throughput
                  claim. No automatic speedup claim is made between unrelated branches.
                </p>
                <p>
                  “Released” includes main commits proven to be ancestors of a semantic-version tag,
                  plus exact tag matches and audited historical backfills of released engines.
                  Release points carry the release version; historical backfills retain their actual
                  harness commit and measurement date in the receipt. Other points retain their
                  measured commit. No timing is inferred for an unmeasured release.{" "}
                  {data.releases.length
                    ? data.releases.map((r) => (
                        <span key={r.name}>
                          <a href={r.url} target="_blank" rel="noreferrer">
                            {r.name} ↗
                          </a>{" "}
                          {benchmark?.points.some((p) => p.sha === r.sha)
                            ? "has measurements in this benchmark."
                            : "has no exact-commit measurement in this benchmark."}{" "}
                        </span>
                      ))
                    : "No version tags are currently available."}
                </p>
                <div className="data-stamp">
                  {data.runCount} runs returned by CodSpeed · {data.excludedRuns}{" "}
                  past-PR/other-branch runs excluded · {data.excludedResults} non-wallclock or
                  invalid results excluded · Retrieved {date(data.fetchedAt)} · Cached up to 5 min
                  (stale responses up to 15 min)
                </div>
              </section>
            )}
          </main>
        </div>
        <footer>
          <span>
            jazz <span className="footer-muted">/ performance lab</span>
          </span>
          <span>Measure. Understand. Improve.</span>
        </footer>
      </div>
    </div>
  );
}
