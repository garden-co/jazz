import { clsx } from "clsx";
import { HeroHeader } from "gcmp-design-system/src/app/components/molecules/HeroHeader";
import dynamic from "next/dynamic";
import { Fragment } from "react";

const LatencyChart = dynamic(() => import("@/components/LatencyChart"), {
  ssr: false,
});

interface DataRow {
  up: boolean;
  latencyOverTime: [number[], number[]];
  avgLatency: number;
  p99Latency: number;
}

const query = async () => {
  const res = await fetch("https://gcmp.grafana.net/api/ds/query", {
    next: { revalidate: 300 },
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
          expr: 'probe_success{instance="https://mesh.jazz.tools/self-sync-check", job="self-sync-check"}',
          instant: true,
          refId: "up",
        },
        {
          datasource: {
            type: "prometheus",
            uid: "grafanacloud-prom",
          },
          expr: '1000 * avg_over_time(probe_duration_seconds{instance="https://mesh.jazz.tools/self-sync-check", job="self-sync-check"}[$__interval]) / 2',
          instant: false,
          range: true,
          interval: "15m",
          refId: "latency_over_time",
        },
        {
          datasource: {
            type: "prometheus",
            uid: "grafanacloud-prom",
          },
          expr: '1000 * avg(avg_over_time(probe_duration_seconds{instance="https://mesh.jazz.tools/self-sync-check", job="self-sync-check"}[$__range])) by (probe) / 2',
          instant: true,
          refId: "avg_latency",
        },
        {
          datasource: {
            type: "prometheus",
            uid: "grafanacloud-prom",
          },
          expr: '1000 * histogram_quantile(0.95, sum(rate(probe_all_duration_seconds_bucket{instance="https://mesh.jazz.tools/self-sync-check", job="self-sync-check"}[$__range])) by (le, probe)) / 2',
          instant: true,
          refId: "p99_latency",
        },
      ],
    }),
  });

  if (!res.ok) {
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

  for (const frame of responseData.results.latency_over_time.frames) {
    const probe = startCase(frame.schema.fields[1].labels.probe);

    byProbe[probe].latencyOverTime = frame.data.values;
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
    Record<string, Record<string, DataRow>>
  >((acc, [label, row]) => {
    switch (label) {
      case "Amsterdam":
      case "Frankfurt":
      case "London":
      case "Paris":
      case "Cape Town":
        return { ...acc, EMEA: { ...acc["EMEA"], [label]: row } };
      case "Atlanta":
      case "Dallas":
      case "New York":
      case "San Francisco":
      case "North Virginia":
      case "Ohio":
      case "Oregon":
      case "Sao Paulo":
      case "Toronto":
        return { ...acc, AMER: { ...acc["AMER"], [label]: row } };
      default:
      case "Sydney":
      case "Tokyo":
      case "Seoul":
      case "Mumbai":
      case "Bangalore":
        return { ...acc, APAC: { ...acc["APAC"], [label]: row } };
    }
  }, {});

  return byRegion;
};

export const metadata = {
  title: "Status",
  description: "Great apps by smart people.",
};

export default async function Page() {
  const byRegion = await query();

  return (
    <div className="container flex flex-col gap-6 pb-10 lg:pb-20">
      <HeroHeader title="Systems status" />

      <table className="min-w-full">
        <thead className="text-left text-sm font-semibold text-stone-900 dark:text-white">
          <tr>
            <th
              scope="col"
              className="py-3.5 pl-4 pr-3 sm:pl-3 w-3/5 hidden md:table-cell"
            >
              Latency (last 7 days)
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
                    colSpan={5}
                    className="py-4 px-3 text-sm font-semibold text-right"
                  >
                    {region}
                  </th>
                </tr>
                {Object.entries(byProbe).map(([label, row]) => (
                  <tr key={label} className="border-t">
                    <td className="px-3 py-4 hidden md:table-cell">
                      <LatencyChart data={row.latencyOverTime} />
                    </td>
                    <td className="whitespace-nowrap px-3 py-4 text-sm">
                      {Math.round(row.avgLatency)} ms
                    </td>
                    <td className="whitespace-nowrap px-3 py-4 text-sm">
                      {Math.round(row.p99Latency)} ms
                    </td>
                    <td className="whitespace-nowrap px-3 py-4 text-sm">
                      <div className="flex items-center gap-2">
                        <div
                          className={clsx(
                            "flex-none rounded-full p-1",
                            row.up
                              ? "text-green-400 bg-green-400/10"
                              : "text-rose-400 bg-rose-400/10",
                          )}
                        >
                          <div className="size-1.5 rounded-full bg-current" />
                        </div>
                        {row.up ? "Up" : "Down"}
                      </div>
                    </td>
                    <td className="whitespace-nowrap px-3 py-4 text-sm">
                      {label}
                    </td>
                  </tr>
                ))}
              </Fragment>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

function startCase(str: string) {
  return str.replace(/([a-z])([A-Z])/g, "$1 $2");
}
