"use client";

import { useEffect, useMemo, useState } from "react";
import { Badge } from "@astryxdesign/core/Badge";
import { Banner } from "@astryxdesign/core/Banner";
import { Card } from "@astryxdesign/core/Card";
import { Divider } from "@astryxdesign/core/Divider";
import { Grid } from "@astryxdesign/core/Grid";
import { Link } from "@astryxdesign/core/Link";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableHeaderCell,
  TableRow,
} from "@astryxdesign/core/Table";
import { Heading, Text } from "@astryxdesign/core/Text";
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

import { basisText, Change, WithHistory } from "./metrics";

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
      <Card>
        <VStack gap={1}>
          <Text type="supporting" display="block">
            {metric.label}
          </Text>
          <Text size="2xl" weight="medium" color="secondary" display="block">
            {loading ? "…" : "—"}
          </Text>
          <Text type="supporting" display="block">
            {loading ? "Reading CodSpeed…" : "No measurement available right now."}
          </Text>
        </VStack>
      </Card>
    );
  const { summary, bench } = entry;
  const previous = summary.history.at(-2);
  const divisor = metric.per?.count ?? 1;
  const time = displayedTime(summary.headline.median / divisor, true);
  const headline = `${time}${metric.per ? ` per ${metric.per.unit}` : ""}`;
  return (
    <WithHistory
      benchmarkId={bench.id}
      name={bench.name}
      summary={summary}
      label={metric.label}
      divisor={divisor}
    >
      <Card
        className="metric-card h-full"
        tabIndex={0}
        role="group"
        aria-label={`${metric.label}: ${headline}. Focus for history.`}
      >
        <VStack gap={1}>
          <Text type="supporting" display="block">
            {metric.label}
          </Text>
          <Text
            size="2xl"
            weight="medium"
            hasTabularNumbers
            display="block"
            className="metric-headline whitespace-nowrap"
          >
            {time}
            {metric.per && (
              <Text color="secondary" weight="normal">
                {" "}
                per {metric.per.unit}
              </Text>
            )}
          </Text>
          <Text type="supporting" hasTabularNumbers display="block">
            {basisText(summary)}
            {previous && (
              <>
                {" · "}
                <Change previous={previous.point.median} current={summary.headline.median} /> vs{" "}
                {summary.basis === "release" ? previous.label : "previous run"}
              </>
            )}
          </Text>
          <Text as="p" display="block">
            {metric.interpret(estimatedSeconds(summary.headline.median), lookup)}
          </Text>
        </VStack>
      </Card>
    </WithHistory>
  );
}

function Video({ example }: { example: HeroExample }) {
  if (!example.video)
    return (
      <Card variant="muted" className="aspect-video">
        <VStack gap={2} hAlign="center" vAlign="center" height="100%">
          <Badge label="Walkthrough coming" />
          <Text as="p" color="secondary" justify="center" display="block">
            {example.plannedVideo}
          </Text>
        </VStack>
      </Card>
    );
  return (
    <VStack as="figure" gap={1}>
      <video
        className="aspect-video w-full rounded-(--radius-container) border border-(--color-border) bg-(--color-background-inverted) object-contain"
        src={example.video.src}
        poster={example.video.poster}
        controls
        muted
        loop
        playsInline
        preload="metadata"
      />
      <figcaption>
        <Text type="supporting" display="block">
          {example.video.caption}
        </Text>
      </figcaption>
    </VStack>
  );
}

function Placeholder({ title, body }: { title: string; body: string }) {
  return (
    <Card variant="muted">
      <VStack gap={1}>
        <HStack gap={2} vAlign="center">
          <Text weight="medium">{title}</Text>
          <Badge label="Planned" />
        </HStack>
        <Text type="supporting" display="block">
          {body}
        </Text>
      </VStack>
    </Card>
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
    <VStack as="section" id={example.id} gap={6} className="scroll-mt-24">
      <Grid columns={{ minWidth: 320, max: 2 }} gap={8}>
        <VStack gap={3}>
          <VStack gap={1}>
            <Heading level={2}>{example.title}</Heading>
            <Text type="large" color="secondary" display="block">
              {example.tagline}
            </Text>
          </VStack>
          <Text as="p" display="block">
            {example.description}
          </Text>
          <ul className="list-disc pl-5">
            {example.highlights.map((highlight) => (
              <li key={highlight}>
                <Text>{highlight}</Text>
              </li>
            ))}
          </ul>
          <HStack gap={4} wrap="wrap">
            {example.sources.map((source) => (
              <Link
                key={source.path}
                href={`${repo}/tree/main/${source.path}`}
                isExternalLink
                hasUnderline
              >
                {source.label}
              </Link>
            ))}
          </HStack>
        </VStack>
        <Video example={example} />
      </Grid>
      <VStack gap={3}>
        <Heading level={3}>Key metrics</Heading>
        {example.plannedMetrics && (
          <Placeholder title="Benchmarks coming" body={example.plannedMetrics} />
        )}
        {example.metrics.length > 0 && (
          <Grid columns={{ minWidth: 240, max: 4 }} gap={3}>
            {example.metrics.map((metric) => (
              <MetricCard
                key={metric.benchmark}
                metric={metric}
                entry={summaries.get(metric.benchmark)}
                lookup={lookup}
                loading={loading}
              />
            ))}
          </Grid>
        )}
        <Grid columns={{ minWidth: 280, max: 2 }} gap={3}>
          <Placeholder
            title="What it costs to run"
            body="Estimated hosting cost for this app at a given number of users, derived from these metrics."
          />
          <Placeholder
            title="Compared with alternatives"
            body="Metrics, cost and code size for the same app built on the closest competing stacks."
          />
        </Grid>
      </VStack>
    </VStack>
  );
}

const suites: [prefix: string, label: string][] = [
  ["crates/jazz/", "Core engine"],
  ["examples/benchmarks/w1/", "Team task board (W1)"],
  ["examples/policy-scoped-documents/", "Policy-scoped documents"],
  ["examples/todo-client-localfirst-ts/", "Todos"],
  ["examples/big-label/", "BigLabel"],
  ["examples/permissioned-resources/", "Permissioned resources"],
  ["examples/band-chat/", "BandChat"],
  ["examples/world-tour/", "World Tour"],
  ["examples/wequencer/", "Wequencer"],
  ["examples/poster-shop/", "PosterShop"],
  ["examples/record-player/", "RecordPlayer"],
  ["examples/chat-react/", "Chat"],
  ["examples/auth-simple-chat/", "Auth chat"],
  ["examples/epic-drop/", "EpicDrop"],
  ["examples/jamazon-warehouse/", "Jamazon Warehouse"],
  ["examples/music-agent/", "MusicAgent"],
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
    <VStack as="section" id="benchmarks" gap={6} className="scroll-mt-24">
      <VStack gap={2}>
        <Heading level={2}>More benchmarks</Heading>
        <Text as="p" color="secondary" display="block" className="max-w-3xl">
          Every other wallclock benchmark we track, grouped by the workload it belongs to. Hover,
          focus or tap a number for its history.
        </Text>
        {loading && (
          <Text type="supporting" display="block">
            Reading CodSpeed…
          </Text>
        )}
      </VStack>
      {groups.map(([suite, entries]) => (
        <VStack key={suite} gap={2}>
          <Heading level={3}>{suite}</Heading>
          <Table density="compact" verticalAlign="top">
            <TableHeader>
              <TableRow>
                <TableHeaderCell>Benchmark</TableHeaderCell>
                <TableHeaderCell>Median</TableHeaderCell>
              </TableRow>
            </TableHeader>
            <TableBody>
              {entries
                .sort((a, b) => a.bench.name.localeCompare(b.bench.name))
                .map(({ bench, summary }) => {
                  const metadata = getBenchmarkMetadata(bench.name);
                  const previous = summary.history.at(-2);
                  const time = displayedTime(summary.headline.median, true);
                  return (
                    <TableRow key={bench.id}>
                      <TableCell>
                        <VStack gap={0.5}>
                          <Text display="block">
                            {metadata?.title ?? bench.name.replaceAll("_", " ")}
                          </Text>
                          <Text type="code" color="secondary" display="block" className="break-all">
                            {bench.name}
                          </Text>
                        </VStack>
                      </TableCell>
                      <TableCell>
                        <WithHistory
                          benchmarkId={bench.id}
                          name={bench.name}
                          summary={summary}
                          label={bench.name}
                          alignment="end"
                        >
                          <div
                            className="benchmark-median"
                            tabIndex={0}
                            role="group"
                            aria-label={`${bench.name}: ${time}. Focus for history.`}
                          >
                            <VStack gap={0.5}>
                              <Text weight="medium" hasTabularNumbers display="block">
                                {time}
                                {previous && (
                                  <>
                                    {" "}
                                    <Change
                                      previous={previous.point.median}
                                      current={summary.headline.median}
                                    />
                                  </>
                                )}
                              </Text>
                              <Text type="supporting" display="block">
                                {metadata
                                  ? `${formatThroughput(summary.headline.median, metadata, true)} · `
                                  : ""}
                                {summary.basis === "release" ? summary.label : "main"}
                              </Text>
                            </VStack>
                          </div>
                        </WithHistory>
                      </TableCell>
                    </TableRow>
                  );
                })}
            </TableBody>
          </Table>
        </VStack>
      ))}
    </VStack>
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
    <div className="mx-auto w-full max-w-[1120px] px-4 pb-24 pt-10 sm:px-8">
      <VStack gap={10}>
        <VStack as="header" gap={4} className="max-w-3xl">
          <Heading level={1}>Real apps, measured on every merge</Heading>
          <Text as="p" type="large" color="secondary" display="block">
            Each example lives in the Jazz repository, most of them as working apps. The numbers
            under it come from benchmarks of that same workload, run on CodSpeed as changes merge to
            main. We show the latest released numbers; hover any of them for how they changed across
            releases.
          </Text>
          {error && <Banner status="error" title={error} />}
          {data && !released && (
            <Banner
              status="info"
              title="Release attribution is unavailable right now, so these are the latest measurements on main."
            />
          )}
          {data && data.warnings.length > 0 && (
            <Banner
              status="warning"
              title="Some benchmark data may be out of date"
              description={data.warnings.join(" ")}
            />
          )}
          <Text as="p" type="supporting" display="block">
            * Estimated times: CodSpeed wallclock medians divided by {ESTIMATE_DIVISOR}, a rough
            allowance for a typical modern machine being faster than the shared CI runner. This is
            illustrative, not a measured prediction for your hardware; every history card also shows
            the measured runner time.
          </Text>
        </VStack>
        {heroExamples.map((example) => (
          <VStack key={example.id} gap={10}>
            <Divider />
            <Hero example={example} summaries={summaries} lookup={lookup} loading={loading} />
          </VStack>
        ))}
        <Divider />
        <MiscBenchmarks summaries={summaries} loading={loading} />
      </VStack>
    </div>
  );
}
