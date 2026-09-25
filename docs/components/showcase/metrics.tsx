"use client";

import { formatTime, plotGeometry } from "@/lib/perf-timeline/model";
import { getBenchmarkMetadata, formatThroughput } from "@/lib/perf-timeline/presentation";
import { change, type MetricSummary } from "@/lib/showcase/summary";

const codspeed = "https://app.codspeed.io/garden-co/jazz";
/** Asks the history explorer to show one benchmark. */
export const selectEvent = "perf-timeline:select";

export function basisText(summary: MetricSummary): string {
  return summary.basis === "release"
    ? `Released in ${summary.label}`
    : `Latest main · ${summary.label}`;
}

export function Change({ previous, current }: { previous: number; current: number }) {
  const ratio = change(previous, current);
  if (!Number.isFinite(ratio) || Math.abs(ratio) < 0.005) return <span>±0%</span>;
  const faster = ratio < 0;
  return (
    <span
      className={
        faster ? "text-emerald-600 dark:text-emerald-400" : "text-amber-700 dark:text-amber-400"
      }
    >
      {faster ? "−" : "+"}
      {Math.abs(ratio * 100).toFixed(ratio > -0.1 && ratio < 0.1 ? 1 : 0)}%
    </span>
  );
}

function Sparkline({ summary }: { summary: MetricSummary }) {
  const points = summary.history.map((h) => h.point);
  const geometry = plotGeometry(points, false, false);
  const x = (i: number) => 6 + geometry.x(i) * 228;
  const y = (v: number) => 6 + geometry.y(v) * 52;
  return (
    <svg viewBox="0 0 240 64" className="h-16 w-full" aria-hidden="true">
      <line x1="6" x2="234" y1="58" y2="58" className="stroke-fd-border" strokeWidth="1" />
      <polyline
        fill="none"
        className="stroke-fd-primary"
        strokeWidth="1.5"
        points={points.map((p, i) => `${x(i)},${y(p.median)}`).join(" ")}
      />
      {points.map((p, i) => (
        <circle
          key={p.resultId}
          cx={x(i)}
          cy={y(p.median)}
          r={i === points.length - 1 ? 3 : 2}
          className="fill-fd-primary"
        />
      ))}
    </svg>
  );
}

/** Hover or focus card with the metric's release (or recent main) history. */
export function HistoryPopover({
  benchmarkId,
  name,
  summary,
  align = "left",
}: {
  benchmarkId: string;
  name: string;
  summary: MetricSummary;
  align?: "left" | "right";
}) {
  const metadata = getBenchmarkMetadata(name);
  const rows = [...summary.history].reverse();
  return (
    <div
      role="tooltip"
      className={`pointer-events-none invisible absolute top-full z-40 mt-2 w-80 max-w-[calc(100vw-2rem)] rounded-lg border border-fd-border bg-fd-popover p-3 text-left text-xs text-fd-popover-foreground opacity-0 shadow-lg transition-opacity group-hover:pointer-events-auto group-hover:visible group-hover:opacity-100 group-focus-within:pointer-events-auto group-focus-within:visible group-focus-within:opacity-100 ${align === "right" ? "right-0" : "left-0"}`}
    >
      <div className="mb-1 font-medium">
        {summary.basis === "release"
          ? "Median by release"
          : "Recent main runs (no release attributed)"}
      </div>
      <Sparkline summary={summary} />
      <table className="mt-2 w-full tabular-nums">
        <tbody>
          {rows.map((entry, i) => {
            const previous = rows[i + 1];
            return (
              <tr key={entry.point.resultId} className="border-t border-fd-border/60">
                <td className="py-1 pr-2 font-mono text-[11px]">{entry.label}</td>
                <td className="py-1 pr-2 text-right">{formatTime(entry.point.median)}</td>
                <td className="py-1 text-right text-fd-muted-foreground">
                  {previous ? (
                    <Change previous={previous.point.median} current={entry.point.median} />
                  ) : (
                    ""
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {summary.unreleased && (
        <p className="mt-2 text-fd-muted-foreground">
          Unreleased main ({summary.unreleased.date.slice(0, 10)}):{" "}
          {formatTime(summary.unreleased.median)}{" "}
          <Change previous={summary.headline.median} current={summary.unreleased.median} />
        </p>
      )}
      {metadata && (
        <p className="mt-2 text-fd-muted-foreground">
          Workload rate: {formatThroughput(summary.headline.median, metadata)}
        </p>
      )}
      <p className="mt-2 flex gap-3">
        <a
          className="underline"
          href="#history"
          onClick={() =>
            window.dispatchEvent(new CustomEvent(selectEvent, { detail: benchmarkId }))
          }
        >
          Full history
        </a>
        <a
          className="underline"
          href={`${codspeed}/benchmarks/${benchmarkId}`}
          target="_blank"
          rel="noreferrer"
        >
          CodSpeed ↗
        </a>
      </p>
    </div>
  );
}
