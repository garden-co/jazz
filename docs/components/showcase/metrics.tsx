"use client";

import type { ReactNode } from "react";
import { HoverCard } from "@astryxdesign/core/HoverCard";
import { Link } from "@astryxdesign/core/Link";
import { VStack } from "@astryxdesign/core/Stack";
import { Table, TableBody, TableCell, TableRow } from "@astryxdesign/core/Table";
import { Text } from "@astryxdesign/core/Text";
import { formatTime, plotGeometry } from "@/lib/perf-timeline/model";
import {
  ESTIMATE_DIVISOR,
  getBenchmarkMetadata,
  displayedTime,
  formatThroughput,
} from "@/lib/perf-timeline/presentation";
import { change, type MetricSummary } from "@/lib/showcase/summary";

const codspeed = "https://app.codspeed.io/garden-co/jazz";

export function basisText(summary: MetricSummary): string {
  return summary.basis === "release"
    ? `Released in ${summary.label}`
    : `Latest main · ${summary.label}`;
}

export function Change({ previous, current }: { previous: number; current: number }) {
  const ratio = change(previous, current);
  if (!Number.isFinite(ratio) || Math.abs(ratio) < 0.005) return <span>±0%</span>;
  const faster = ratio < 0;
  // The sign carries the meaning; colour only reinforces it.
  return (
    <span className={faster ? "text-(--color-text-green)" : "text-(--color-text-orange)"}>
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
      <line x1="6" x2="234" y1="58" y2="58" stroke="var(--color-border)" strokeWidth="1" />
      <polyline
        fill="none"
        stroke="var(--color-icon-accent)"
        strokeWidth="1.5"
        points={points.map((p, i) => `${x(i)},${y(p.median)}`).join(" ")}
      />
      {points.map((p, i) => (
        <circle
          key={p.resultId}
          cx={x(i)}
          cy={y(p.median)}
          r={i === points.length - 1 ? 3 : 2}
          fill="var(--color-icon-accent)"
        />
      ))}
    </svg>
  );
}

/** The metric's release (or recent main) history, shown in a hover card. */
function History({
  benchmarkId,
  name,
  summary,
  divisor,
}: {
  benchmarkId: string;
  name: string;
  summary: MetricSummary;
  divisor: number;
}) {
  const metadata = getBenchmarkMetadata(name);
  const rows = [...summary.history].reverse();
  return (
    <VStack gap={2} className="w-80 max-w-[calc(100vw-2rem)]">
      <Text type="label" display="block">
        {summary.basis === "release"
          ? "Median by release"
          : "Recent main runs (no release attributed)"}
      </Text>
      <Sparkline summary={summary} />
      <Table density="compact">
        <TableBody>
          {rows.map((entry, i) => {
            const previous = rows[i + 1];
            return (
              <TableRow key={entry.point.resultId}>
                <TableCell>
                  <Text type="code">{entry.label}</Text>
                </TableCell>
                <TableCell>
                  <Text hasTabularNumbers>{displayedTime(entry.point.median / divisor, true)}</Text>
                </TableCell>
                <TableCell>
                  {previous && (
                    <Text hasTabularNumbers>
                      <Change previous={previous.point.median} current={entry.point.median} />
                    </Text>
                  )}
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
      {summary.unreleased && (
        <Text type="supporting" display="block">
          Unreleased main ({summary.unreleased.date.slice(0, 10)}):{" "}
          {displayedTime(summary.unreleased.median / divisor, true)}{" "}
          <Change previous={summary.headline.median} current={summary.unreleased.median} />
        </Text>
      )}
      <Text type="supporting" display="block">
        Measurement on the de-noised CodSpeed environment (about {ESTIMATE_DIVISOR}x slower than a
        normal CPU): {formatTime(summary.headline.median / divisor)}
      </Text>
      {metadata && (
        <Text type="supporting" display="block">
          What is timed: {metadata.description} {metadata.fixture}
        </Text>
      )}
      {metadata && (
        <Text type="supporting" display="block">
          Workload rate: {formatThroughput(summary.headline.median, metadata, true)}
        </Text>
      )}
      <Link href={`${codspeed}/benchmarks/${benchmarkId}`} isExternalLink>
        Full history on CodSpeed
      </Link>
    </VStack>
  );
}

/**
 * Shows a metric's history when its trigger is hovered, focused or tapped.
 * The trigger must be focusable (tabIndex) so keyboard users reach it too.
 */
export function WithHistory({
  benchmarkId,
  name,
  summary,
  label,
  alignment = "start",
  divisor = 1,
  children,
}: {
  benchmarkId: string;
  name: string;
  summary: MetricSummary;
  label: string;
  alignment?: "start" | "end";
  /** Shows per-operation times: run seconds ÷ divisor. */
  divisor?: number;
  children: ReactNode;
}) {
  return (
    <HoverCard
      label={`History of ${label}`}
      placement="below"
      alignment={alignment}
      focusTrigger="always"
      touchTrigger="tap"
      hasHoverIndication={false}
      content={
        <History benchmarkId={benchmarkId} name={name} summary={summary} divisor={divisor} />
      }
    >
      {children}
    </HoverCard>
  );
}
