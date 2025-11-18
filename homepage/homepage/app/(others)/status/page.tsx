import LatencyChart from "@/components/LatencyChart";
import { HeroHeader } from "@garden-co/design-system/src/components/molecules/HeroHeader";
import { clsx } from "clsx";
import type { Metadata } from "next";
import { Fragment } from "react";

const metaTags = {
  title: "Status",
  description: "View Jazz system status and latency across probes",
  url: "https://jazz.tools",
}

export const dynamic = "force-static";

export const metadata: Metadata = {
  title: metaTags.title,
  description: metaTags.description,
  openGraph: {
    title: metaTags.title,
    description: metaTags.description,
    images: [
      {
        url: `${metaTags.url}/api/opengraph-image?title=${encodeURIComponent(metaTags.title)}`,
        height: 630,
        alt: metaTags.title,
      },
    ],
  },
};

const PROBES = [
  "NorthCalifornia",
  "NorthVirginia",
  "Montreal",
  "SaoPaulo",
  "Mumbai",
  "Singapore",
  "Sydney",
  "Tokyo",
  "CapeTown",
  "London",
  "Spain",
  "UAE",
  "Zurich",
  "Frankfurt",
] as const;

interface DataRow {
  up: boolean;
  upRatio: number;
  upOverTime: [number[], number[]];
  upCountOverTime: [number[], number[]];
  latencyOverTime: [number[], number[]];
  avgLatency: number;
  p99Latency: number;
}

const intervalMin = 120;

const query = async () => {
  const res = await fetch("https://gcmp.grafana.net/api/ds/query", {
    cache: "force-cache",
    next: {
      revalidate: 300,
    },
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.GRAFANA_SERVICE_ACCOUNT}`,
    },
    body: JSON.stringify({
      from: "now-7d",
      to: "now",
      queries: [
        {
          datasource: {
            type: "prometheus",
            uid: "grafanacloud-prom",
          },
          expr: `probe_success{instance="https://mesh.jazz.tools/self-sync-check", job="self-sync-check", probe=~"${PROBES.join("|")}"}`,
          instant: true,
          refId: "up",
        },
        {
          datasource: {
            type: "prometheus",
            uid: "grafanacloud-prom",
          },
          expr: `sum  by (probe) (sum_over_time(probe_success{instance="https://mesh.jazz.tools/self-sync-check", job="self-sync-check", probe=~"${PROBES.join("|")}"}[$__range]))
          /
          sum  by (probe) (count_over_time(probe_success{instance="https://mesh.jazz.tools/self-sync-check", job="self-sync-check", probe=~"${PROBES.join("|")}"}[$__range]))`,
          instant: true,
          range: false,
          refId: "up_ratio",
        },
        {
          datasource: {
            type: "prometheus",
            uid: "grafanacloud-prom",
          },
          expr: `sum by (probe) (sum_over_time(probe_success{instance="https://mesh.jazz.tools/self-sync-check", job="self-sync-check", probe=~"${PROBES.join("|")}"}[$__interval]))`,
          instant: false,
          range: true,
          interval: intervalMin + "m",
          refId: "up_over_time",
        },
        {
          datasource: {
            type: "prometheus",
            uid: "grafanacloud-prom",
          },
          expr: `sum by (probe) (count_over_time(probe_success{instance="https://mesh.jazz.tools/self-sync-check", job="self-sync-check", probe=~"${PROBES.join("|")}"}[$__interval]))`,
          instant: false,
          range: true,
          interval: intervalMin + "m",
          refId: "up_count_over_time",
        },
        {
          datasource: {
            type: "prometheus",
            uid: "grafanacloud-prom",
          },
          expr: `1000 * sum by (probe) (avg_over_time(probe_duration_seconds{instance="https://mesh.jazz.tools/self-sync-check", job="self-sync-check", probe=~"${PROBES.join("|")}"}[$__interval])) / 2`,
          instant: false,
          range: true,
          interval: intervalMin + "m",
          refId: "latency_over_time",
        },
        {
          datasource: {
            type: "prometheus",
            uid: "grafanacloud-prom",
          },
          expr: `1000 * avg(avg_over_time(probe_duration_seconds{instance="https://mesh.jazz.tools/self-sync-check", job="self-sync-check", probe=~"${PROBES.join("|")}"}[$__range])) by (probe) / 2`,
          instant: true,
          refId: "avg_latency",
        },
        {
          datasource: {
            type: "prometheus",
            uid: "grafanacloud-prom",
          },
          expr: `1000 * histogram_quantile(0.95, sum(rate(probe_all_duration_seconds_bucket{instance="https://mesh.jazz.tools/self-sync-check", job="self-sync-check", probe=~"${PROBES.join("|")}"}[$__range])) by (le, probe)) / 2`,
          instant: true,
          refId: "p99_latency",
        },
      ],
    }),
  });

  if (!res.ok) {
    console.error(res);
    console.error(await res.text());

    return;
  }

  const responseData = await res.json();

  const byProbe: Record<string, DataRow> = {};

  for (const frame of responseData.results.up.frames) {
    const probe = startCase(frame.schema.fields[1].labels.probe);
    byProbe[probe] = {
      ...byProbe[probe],
      up: frame.data.values[1][0] === 1,
    };
  }

  for (const frame of responseData.results.up_ratio.frames) {
    const probe = startCase(frame.schema.fields[1].labels.probe);
    byProbe[probe].upRatio = frame.data.values[1][0];
  }

  for (const frame of responseData.results.latency_over_time.frames) {
    const probe = startCase(frame.schema.fields[1].labels.probe);
    byProbe[probe].latencyOverTime = frame.data.values;
  }

  for (const frame of responseData.results.up_over_time.frames) {
    const probe = startCase(frame.schema.fields[1].labels.probe);
    byProbe[probe].upOverTime = frame.data.values;
  }

  for (const frame of responseData.results.up_count_over_time.frames) {
    const probe = startCase(frame.schema.fields[1].labels.probe);
    byProbe[probe].upCountOverTime = frame.data.values;
  }

  for (const frame of responseData.results.avg_latency.frames) {
    const probe = startCase(frame.schema.fields[1].labels.probe);
    byProbe[probe].avgLatency = frame.data.values[1];
  }

  for (const frame of responseData.results.p99_latency.frames) {
    const probe = startCase(frame.schema.fields[1].labels.probe);
    byProbe[probe].p99Latency = frame.data.values[1];
  }

  const byRegion = Object.entries(byProbe).reduce<
    Record<string, Record<"EMEA" | "AMER" | "APAC", DataRow>>
  >((acc, [label, row]) => {
    switch (label) {
      case "London":
      case "Cape Town":
      case "Spain":
      case "Zurich":
      case "UAE":
      case "Frankfurt":
        return { ...acc, EMEA: { ...acc["EMEA"], [label]: row } };
      case "North Virginia":
      case "North California":
      case "Montreal":
      case "Sao Paulo":
        return { ...acc, AMER: { ...acc["AMER"], [label]: row } };
      default:
      case "Mumbai":
      case "Sydney":
      case "Tokyo":
      case "Singapore":
        return { ...acc, APAC: { ...acc["APAC"], [label]: row } };
    }
  }, {});

  return byRegion;
};

export default async function Page() {
  const byRegion = await query();

  return (
    <div className="container flex flex-col gap-6 pb-10 lg:pb-20">
      <HeroHeader title="Systems status" />

      <table className="min-w-full">
        <thead className="text-left text-sm font-semibold text-highlight">
          <tr>
            <th
              scope="col"
              className="py-3.5 pl-4 pr-3 sm:pl-3 w-3/5 hidden md:table-cell"
            >
              Latency & uptime (last 7 days)
            </th>
            <th scope="col" className="px-3 py-3.5">
              Average <span className="md:hidden">latency</span>
            </th>
            <th scope="col" className="px-3 py-3.5 whitespace-nowrap">
              p99 <span className="md:hidden">latency</span>
            </th>
            <th scope="col" className="px-3 py-3.5">
              Status
            </th>
            <th scope="col" className="px-3 py-3.5">
              Uptime
            </th>
            <th>
              <span className="sr-only">Location</span>
            </th>
          </tr>
        </thead>
        <tbody>
          {!byRegion ? (
            <tr>
              <td colSpan={5} className="py-4 px-3 text-sm text-center">
                No data. Please try again later.
              </td>
            </tr>
          ) : (
            Object.entries(byRegion).map(([region, byProbe]) => (
              <Fragment key={region}>
                <tr>
                  <th
                    colSpan={6}
                    className="py-2 px-3 text-sm font-semibold text-right"
                  >
                    {region}
                  </th>
                </tr>
                {Object.entries(byProbe).map(([label, row]) => (
                  <tr key={label} className="border-t">
                    <td className="px-3 py-2 hidden md:table-cell">
                      <LatencyChart
                        latencyOverTime={row.latencyOverTime}
                        upOverTime={row.upOverTime}
                        upCountOverTime={row.upCountOverTime}
                        intervalMin={intervalMin}
                        isUp={row.up}
                      />
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-sm">
                      {Math.round(row.avgLatency)} ms
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-sm">
                      {Math.round(row.p99Latency)} ms
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-sm">
                      <div className="flex items-center gap-2">
                        <div
                          className={clsx(
                            "flex-none rounded-full p-1",
                            row.up
                              ? "text-green-400 bg-green-400/30"
                              : "text-red-500 bg-red-400/30",
                          )}
                        >
                          <div className="size-1.5 rounded-full bg-current" />
                        </div>
                        {row.up ? "Up" : "Down"}
                      </div>
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-sm">
                      <span className="text-xs text-gray-500">
                        {formatUptime(row.upRatio)}
                      </span>
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-sm">
                      {label}
                    </td>
                  </tr>
                ))}
              </Fragment>
            ))
          )}
        </tbody>
      </table>
        <p className="text-sm text-gray-500 text-right">
          All times are expressed in <span className="font-bold">UTC</span>
        </p>
    </div>
  );
}

function formatUptime(upRatio: number) {
  if (upRatio === 1) return "100%";

  const upPercentage = upRatio * 100;

  // count leading nines and truncate after first non-nine
  const firstNonNine = upPercentage.toString().split("").findIndex((char) => char !== "9" && char !== ".");
  if (firstNonNine === 1) {
    return upPercentage.toString().slice(0, 4) + "%";
  }
  return upPercentage.toString().slice(0, firstNonNine + 1) + "%";
}

function startCase(str: string) {
  return str.replace(/([a-z])([A-Z])/g, "$1 $2");
}
