"use client";

import { useEffect, useMemo, useState } from "react";
import { fetchTimeline } from "@/lib/perf-timeline/client";
import type { Benchmark, Timeline } from "@/lib/perf-timeline/model";
import {
  getBenchmarkMetadata,
  displayedTime,
  estimatedSeconds,
  ESTIMATE_DIVISOR,
  formatThroughput,
} from "@/lib/perf-timeline/presentation";
import {
  heroBenchmarkNames,
  heroExamples,
  type HeroExample,
  type Lookup,
} from "@/lib/showcase/catalogue";
import { summarize, type MetricSummary } from "@/lib/showcase/summary";

import { basisText, Change, HistoryPopover } from "./metrics";

const repo = "https://github.com/garden-co/jazz";

type Summaries = Map<string, { bench: Benchmark; summary: MetricSummary }>;

function useSummaries() {
  const [data, setData] = useState<Timeline | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    fetchTimeline().then(setData, (cause: Error) => setError(cause.message));
  }, []);
  const summaries = useMemo(() => {
    const byName: Summaries = new Map();
    for (const bench of data?.benchmarks ?? []) {
      const summary = summarize(bench);
      // Names can repeat across retired IDs; keep the one with the newest data.
      const existing = byName.get(bench.name);
      if (summary && (!existing || existing.summary.headline.date < summary.headline.date))
        byName.set(bench.name, { bench, summary });
    }
    return byName;
  }, [data]);
  return { data, error, summaries };
}

function MetricCard({
  metric,
  entry,
  lookup,
  loading,
}: {
  metric: HeroExample["metrics"][number];
  entry: { bench: Benchmark; summary: MetricSummary } | undefined;
  lookup: Lookup;
  loading: boolean;
}) {
  if (!entry)
    return (
      <div className="rounded-xl border border-fd-border bg-fd-card p-4">
        <div className="text-sm text-fd-muted-foreground">{metric.label}</div>
        <div className="mt-2 text-2xl font-medium text-fd-muted-foreground">
          {loading ? "…" : "—"}
        </div>
        <p className="mt-2 text-xs text-fd-muted-foreground">
          {loading ? "Reading CodSpeed…" : "No measurement available right now."}
        </p>
      </div>
    );
  const { summary, bench } = entry;
  const previous = summary.history.at(-2);
  const divisor = metric.per?.count ?? 1;
  const headline = `${displayedTime(summary.headline.median / divisor, true)}${metric.per ? ` per ${metric.per.unit}` : ""}`;
  return (
    <div
      className="group relative rounded-xl border border-fd-border bg-fd-card p-4 outline-none focus-visible:ring-2 focus-visible:ring-fd-primary"
      tabIndex={0}
      aria-label={`${metric.label}: ${headline}. Focus for history.`}
    >
      <div className="text-sm text-fd-muted-foreground">{metric.label}</div>
      <div className="mt-1 whitespace-nowrap text-2xl font-medium tabular-nums">
        {displayedTime(summary.headline.median / divisor, true)}
        {metric.per && (
          <span className="ml-1.5 text-base font-normal text-fd-muted-foreground">
            per {metric.per.unit}
          </span>
        )}
      </div>
      <div className="mt-0.5 font-mono text-[11px] text-fd-muted-foreground">
        {basisText(summary)}
        {previous && (
          <>
            {" · "}
            <Change previous={previous.point.median} current={summary.headline.median} /> vs{" "}
            {summary.basis === "release" ? previous.label : "previous run"}
          </>
        )}
      </div>
      <p className="mt-3 text-sm leading-relaxed">
        {metric.interpret(estimatedSeconds(summary.headline.median), lookup)}
      </p>
      <HistoryPopover
        benchmarkId={bench.id}
        name={bench.name}
        summary={summary}
        divisor={divisor}
      />
    </div>
  );
}

function Video({ example }: { example: HeroExample }) {
  if (!example.video)
    return (
      <div className="flex aspect-video flex-col items-center justify-center rounded-xl border border-dashed border-fd-border bg-fd-muted/40 p-6 text-center">
        <span className="rounded-full border border-fd-border px-2 py-0.5 text-[11px] uppercase tracking-wider text-fd-muted-foreground">
          Walkthrough coming
        </span>
        <p className="mt-3 max-w-sm text-sm text-fd-muted-foreground">{example.plannedVideo}</p>
      </div>
    );
  return (
    <figure>
      <video
        className="aspect-video w-full rounded-xl border border-fd-border bg-black object-contain"
        src={example.video.src}
        poster={example.video.poster}
        controls
        muted
        loop
        playsInline
        preload="metadata"
      />
      <figcaption className="mt-2 text-xs text-fd-muted-foreground">
        {example.video.caption}
      </figcaption>
    </figure>
  );
}

function Placeholder({ title, body }: { title: string; body: string }) {
  return (
    <div className="rounded-xl border border-dashed border-fd-border p-4">
      <div className="flex items-center gap-2 text-sm font-medium">
        {title}
        <span className="rounded-full border border-fd-border px-2 py-0.5 text-[10px] font-normal uppercase tracking-wider text-fd-muted-foreground">
          Planned
        </span>
      </div>
      <p className="mt-1 text-xs text-fd-muted-foreground">{body}</p>
    </div>
  );
}

function Hero({
  example,
  summaries,
  lookup,
  loading,
}: {
  example: HeroExample;
  summaries: Summaries;
  lookup: Lookup;
  loading: boolean;
}) {
  return (
    <section id={example.id} className="scroll-mt-24 border-t border-fd-border py-14">
      <div className="grid gap-8 lg:grid-cols-[minmax(0,5fr)_minmax(0,6fr)]">
        <div>
          <h2 className="text-3xl font-medium tracking-tight">{example.title}</h2>
          <p className="mt-2 text-lg text-fd-muted-foreground">{example.tagline}</p>
          <p className="mt-4 leading-relaxed">{example.description}</p>
          <ul className="mt-4 space-y-1.5 text-sm">
            {example.highlights.map((highlight) => (
              <li key={highlight} className="flex gap-2">
                <span className="text-fd-primary" aria-hidden="true">
                  →
                </span>
                {highlight}
              </li>
            ))}
          </ul>
          <p className="mt-5 flex flex-wrap gap-x-4 gap-y-1 text-sm">
            {example.sources.map((source) => (
              <a
                key={source.path}
                className="underline decoration-fd-border underline-offset-4 hover:decoration-fd-primary"
                href={`${repo}/tree/main/${source.path}`}
                target="_blank"
                rel="noreferrer"
              >
                {source.label} ↗
              </a>
            ))}
          </p>
        </div>
        <Video example={example} />
      </div>
      <h3 className="mt-10 text-sm font-medium uppercase tracking-wider text-fd-muted-foreground">
        Key metrics
      </h3>
      {example.plannedMetrics && (
        <div className="mt-3">
          <Placeholder title="Benchmarks coming" body={example.plannedMetrics} />
        </div>
      )}
      <div className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {example.metrics.map((metric) => (
          <MetricCard
            key={metric.benchmark}
            metric={metric}
            entry={summaries.get(metric.benchmark)}
            lookup={lookup}
            loading={loading}
          />
        ))}
      </div>
      <div className="mt-3 grid gap-3 sm:grid-cols-2">
        <Placeholder
          title="What it costs to run"
          body="Estimated hosting cost for this app at a given number of users, derived from these metrics."
        />
        <Placeholder
          title="Compared with alternatives"
          body="Metrics, cost and code size for the same app built on the closest competing stacks."
        />
      </div>
    </section>
  );
}

const suites: [prefix: string, label: string][] = [
  ["crates/jazz/", "Core engine"],
  ["examples/benchmarks/w1/", "Team task board (W1)"],
  ["examples/policy-scoped-documents/", "Policy-scoped documents"],
  ["examples/todo-client-localfirst-ts/", "Todos"],
  ["examples/big-label/", "BigLabel"],
  ["examples/permissioned-resources/", "Permissioned resources"],
];
const otherSuite = "Other benchmarks";

function suiteOf(name: string): string {
  const source = getBenchmarkMetadata(name)?.source;
  return suites.find(([prefix]) => source?.startsWith(prefix))?.[1] ?? otherSuite;
}

function MiscBenchmarks({ summaries, loading }: { summaries: Summaries; loading: boolean }) {
  const groups = useMemo(() => {
    const grouped = new Map<string, { bench: Benchmark; summary: MetricSummary }[]>();
    for (const [name, entry] of summaries) {
      if (heroBenchmarkNames.has(name)) continue;
      const suite = suiteOf(name);
      grouped.set(suite, [...(grouped.get(suite) ?? []), entry]);
    }
    const order = [...suites.map(([, label]) => label), otherSuite];
    return [...grouped.entries()].sort(([a], [b]) => order.indexOf(a) - order.indexOf(b));
  }, [summaries]);
  return (
    <section id="benchmarks" className="scroll-mt-24 border-t border-fd-border py-14">
      <h2 className="text-3xl font-medium tracking-tight">More benchmarks</h2>
      <p className="mt-2 max-w-3xl text-fd-muted-foreground">
        Every other wallclock benchmark we track, grouped by the workload it belongs to. Hover or
        focus a number for its history.
      </p>
      {loading && <p className="mt-6 text-sm text-fd-muted-foreground">Reading CodSpeed…</p>}
      {groups.map(([suite, entries]) => (
        <div key={suite} className="mt-8">
          <h3 className="text-sm font-medium uppercase tracking-wider text-fd-muted-foreground">
            {suite}
          </h3>
          <div className="mt-2 divide-y divide-fd-border rounded-xl border border-fd-border">
            {entries
              .sort((a, b) => a.bench.name.localeCompare(b.bench.name))
              .map(({ bench, summary }) => {
                const metadata = getBenchmarkMetadata(bench.name);
                const previous = summary.history.at(-2);
                return (
                  <div
                    key={bench.id}
                    className="flex flex-wrap items-center justify-between gap-x-6 gap-y-1 px-4 py-2.5"
                  >
                    <div className="min-w-0 flex-1">
                      <div className="text-sm">
                        {metadata?.title ?? bench.name.replaceAll("_", " ")}
                      </div>
                      <div className="truncate font-mono text-[11px] text-fd-muted-foreground">
                        {bench.name}
                      </div>
                    </div>
                    <div
                      className="group relative text-right outline-none focus-visible:ring-2 focus-visible:ring-fd-primary"
                      tabIndex={0}
                      aria-label={`${bench.name}: ${displayedTime(summary.headline.median, true)}. Focus for history.`}
                    >
                      <div className="text-sm font-medium tabular-nums">
                        {displayedTime(summary.headline.median, true)}
                        {previous && (
                          <span className="ml-2 text-xs font-normal">
                            <Change
                              previous={previous.point.median}
                              current={summary.headline.median}
                            />
                          </span>
                        )}
                      </div>
                      <div className="text-[11px] text-fd-muted-foreground">
                        {metadata
                          ? `${formatThroughput(summary.headline.median, metadata, true)} · `
                          : ""}
                        {summary.basis === "release" ? summary.label : "main"}
                      </div>
                      <HistoryPopover
                        benchmarkId={bench.id}
                        name={bench.name}
                        summary={summary}
                        align="right"
                      />
                    </div>
                  </div>
                );
              })}
          </div>
        </div>
      ))}
    </section>
  );
}

export function Showcase() {
  const { data, error, summaries } = useSummaries();
  const loading = !data && !error;
  const lookup: Lookup = (name) => {
    const seconds = summaries.get(name)?.summary.headline.median;
    return seconds === undefined ? null : estimatedSeconds(seconds);
  };
  const released = [...summaries.values()].some((entry) => entry.summary.basis === "release");

  return (
    <main className="mx-auto w-full max-w-[1400px] px-4 pt-14 sm:px-8">
      <header className="max-w-3xl">
        <div className="font-mono text-xs uppercase tracking-widest text-fd-muted-foreground">
          Examples &amp; benchmarks
        </div>
        <h1 className="mt-3 text-4xl font-medium tracking-tight sm:text-5xl">
          Real apps, measured on every commit.
        </h1>
        <p className="mt-4 text-lg text-fd-muted-foreground">
          Each example is a working app in the Jazz repository. The numbers under it come from
          benchmarks of that same workload, run in CI on CodSpeed. We show the latest released
          numbers; hover any of them for how they changed across releases.
        </p>
        <nav className="mt-6 flex flex-wrap gap-2 text-sm" aria-label="Examples">
          {heroExamples.map((example) => (
            <a
              key={example.id}
              href={`#${example.id}`}
              className="rounded-full border border-fd-border px-3 py-1 hover:border-fd-primary"
            >
              {example.title}
            </a>
          ))}
          <a
            href="#benchmarks"
            className="rounded-full border border-fd-border px-3 py-1 hover:border-fd-primary"
          >
            More benchmarks
          </a>
        </nav>
        {error && (
          <p
            className="mt-6 rounded-lg border border-fd-border bg-fd-muted p-3 text-sm"
            role="alert"
          >
            {error}
          </p>
        )}
        {data && !released && (
          <p
            className="mt-6 rounded-lg border border-fd-border bg-fd-muted p-3 text-sm"
            role="status"
          >
            Release attribution is unavailable right now, so these are the latest measurements on
            main.
          </p>
        )}
        <p className="mt-6 text-xs text-fd-muted-foreground">
          * Estimated times: CodSpeed wallclock medians divided by {ESTIMATE_DIVISOR}, a rough
          allowance for a typical modern machine being faster than the shared CI runner. This is
          illustrative, not a measured prediction for your hardware; every popover also shows the
          measured runner time.
        </p>
      </header>
      <div className="mt-10">
        {heroExamples.map((example) => (
          <Hero
            key={example.id}
            example={example}
            summaries={summaries}
            lookup={lookup}
            loading={loading}
          />
        ))}
      </div>
      <MiscBenchmarks summaries={summaries} loading={loading} />
    </main>
  );
}
