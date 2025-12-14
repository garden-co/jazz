import { useEffect, useState } from "react";
import type {
  DataPoint,
  Histogram,
  ResourceMetrics,
  ScopeMetrics,
} from "@opentelemetry/sdk-metrics";
import { styled } from "goober";
import { CoID, RawCoValue } from "cojson";
import { jazzMetricReader } from "../utils/otel";
import { Accordion } from "../ui/accordion";
import { DataTable, type ColumnDef } from "../ui/data-table";
import { Heading } from "../ui/heading";
import { Text } from "../ui/text";
import { Badge } from "../ui/badge";
import { useRouter } from "../router";

function getSumOfCounterMetric(
  metrics: ScopeMetrics[],
  scope: string,
  name: string,
  attributes?: Record<string, string>,
) {
  const dp = metrics
    .find((sm) => sm.scope.name === scope)
    ?.metrics.find((m) => m.descriptor.name === name)?.dataPoints;

  if (!dp) {
    return 0;
  }

  return dp.reduce((acc, dp) => {
    if (typeof dp.value !== "number") {
      throw new Error(`Metric ${name} has a value that is not a number`);
    }

    // if attributes is defined, and the attributes do not match, skip this counter
    if (
      attributes &&
      !Object.keys(attributes).every(
        (key) => dp.attributes[key] === attributes[key],
      )
    ) {
      return acc;
    }

    return acc + dp.value;
  }, 0);
}

function getActiveSubscriptions(metrics: ScopeMetrics[]) {
  const scope = "jazz-tools";
  const name = "jazz.subscription.active";

  const dp = metrics
    .find((sm) => sm.scope.name === scope)
    ?.metrics.find((m) => m.descriptor.name === name)?.dataPoints as
    | DataPoint<number>[]
    | undefined;

  if (!dp) {
    return [];
  }

  return dp
    .filter((dp) => dp.value > 0)
    .map((dp) => ({
      id: dp.attributes.id as string,
      source_id: dp.attributes.source_id as string | undefined,
      value: dp.value,
    }));
}

function getLoadTimes(metrics: ScopeMetrics[]) {
  const scope = "jazz-tools";
  const name = "jazz.subscription.first_load";

  const dp = metrics
    .find((sm) => sm.scope.name === scope)
    ?.metrics.find((m) => m.descriptor.name === name)?.dataPoints as
    | DataPoint<Histogram>[]
    | undefined;

  if (!dp) {
    return [];
  }

  return dp.map((dp) => ({
    id: dp.attributes.id as string,
    source_id: dp.attributes.source_id as string | undefined,
    parent_id: dp.attributes.parent_id as string | undefined,
    resolve: dp.attributes.resolve as string | undefined,
    loadTime: dp.value.min ?? 0,
  }));
}

type PerfMetrics = {
  transport: {
    ingress: number;
    egress: number;
  };
  cojson: {
    available: number;
    loading: number;
    unknown: number;
    unavailable: number;
  };
  tools: {
    activeSubscriptions: {
      id: string;
      source_id?: string;
      value: number;
    }[];
    loadTimes: {
      id: string;
      source_id?: string;
      parent_id?: string;
      resolve?: string;
      loadTime: number;
    }[];
  };
};

function exportMetrics(resourceMetrics: ResourceMetrics[]): PerfMetrics | null {
  const scopedMetrics = resourceMetrics.at(0)?.scopeMetrics;

  if (!scopedMetrics) {
    return null;
  }

  const ingress = getSumOfCounterMetric(
    scopedMetrics,
    "cojson-transport-ws",
    "jazz.usage.ingress",
  );
  const egress = getSumOfCounterMetric(
    scopedMetrics,
    "cojson-transport-ws",
    "jazz.usage.egress",
  );
  const availableCoValues = getSumOfCounterMetric(
    scopedMetrics,
    "cojson",
    "jazz.covalues.loaded",
    {
      state: "available",
    },
  );
  const loadingCoValues = getSumOfCounterMetric(
    scopedMetrics,
    "cojson",
    "jazz.covalues.loaded",
    {
      state: "loading",
    },
  );

  const unknownCoValues = getSumOfCounterMetric(
    scopedMetrics,
    "cojson",
    "jazz.covalues.loaded",
    {
      state: "unknown",
    },
  );

  const unavailableCoValues = getSumOfCounterMetric(
    scopedMetrics,
    "cojson",
    "jazz.covalues.loaded",
    {
      state: "unavailable",
    },
  );

  return {
    transport: {
      ingress,
      egress,
    },
    cojson: {
      available: availableCoValues,
      loading: loadingCoValues,
      unknown: unknownCoValues,
      unavailable: unavailableCoValues,
    },
    tools: {
      loadTimes: getLoadTimes(scopedMetrics),
      activeSubscriptions: getActiveSubscriptions(scopedMetrics),
    },
  };
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
}

function formatTime(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(2)} μs`;
  if (ms < 1000) return `${ms.toFixed(2)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

export default function Perf() {
  const [metricsError, setMetricsError] = useState<Error | null>(null);
  const [metrics, setMetrics] = useState<PerfMetrics | null>(null);
  const { addPages } = useRouter();

  useEffect(() => {
    async function fetchMetrics() {
      try {
        const metrics = await jazzMetricReader.collectMetrics();
        setMetrics(exportMetrics(metrics));
      } catch (error) {
        setMetricsError(error as Error);
      }
    }

    const interval = setInterval(fetchMetrics, 1_000);
    fetchMetrics();
    return () => clearInterval(interval);
  }, []);

  if (metricsError) {
    return (
      <DashboardContainer>
        <Text>Error fetching metrics: {metricsError.message}</Text>
      </DashboardContainer>
    );
  }

  if (!metrics) {
    return (
      <DashboardContainer>
        <Text>Loading metrics...</Text>
      </DashboardContainer>
    );
  }

  const loadTimeColumns: ColumnDef<
    PerfMetrics["tools"]["loadTimes"][number]
  >[] = [
    {
      id: "loadTime",
      header: "Load Time",
      accessor: (row) => formatTime(row.loadTime),
      sortable: true,
      sortFn: (a, b) => a.loadTime - b.loadTime,
    },
    {
      id: "id",
      header: "CoValue ID",
      accessor: (row) => (
        <ClickableId
          onClick={() =>
            addPages([{ coId: row.id as CoID<RawCoValue>, name: row.id }])
          }
        >
          <Text mono small>
            {row.id}
          </Text>
        </ClickableId>
      ),
      sortable: true,
    },
    {
      id: "source_id",
      header: "Initiator",
      accessor: (row) =>
        row.source_id ? (
          <ClickableId
            onClick={() =>
              addPages([
                {
                  coId: row.source_id as CoID<RawCoValue>,
                  name: row.source_id,
                },
              ])
            }
          >
            <Text mono small>
              {row.source_id}
            </Text>
          </ClickableId>
        ) : (
          <Text muted small>
            —
          </Text>
        ),
      sortable: true,
    },
    {
      id: "parent_id",
      header: "Parent ID",
      accessor: (row) =>
        row.parent_id ? (
          <ClickableId
            onClick={() =>
              addPages([
                {
                  coId: row.parent_id as CoID<RawCoValue>,
                  name: row.parent_id,
                },
              ])
            }
          >
            <Text mono small>
              {row.parent_id}
            </Text>
          </ClickableId>
        ) : (
          <Text muted small>
            —
          </Text>
        ),
      sortable: true,
    },
    {
      id: "resolve",
      header: "Resolve",
      accessor: (row) =>
        row.resolve ? (
          <Text small>{row.resolve}</Text>
        ) : (
          <Text muted small>
            —
          </Text>
        ),
      sortable: true,
    },
  ];

  const subscriptionColumns: ColumnDef<
    PerfMetrics["tools"]["activeSubscriptions"][number]
  >[] = [
    {
      id: "id",
      header: "CoValue ID",
      accessor: (row) => (
        <ClickableId
          onClick={() =>
            addPages([{ coId: row.id as CoID<RawCoValue>, name: row.id }])
          }
        >
          <Text mono small>
            {row.id}
          </Text>
        </ClickableId>
      ),
      sortable: true,
    },
    {
      id: "source_id",
      header: "Source ID",
      accessor: (row) =>
        row.source_id ? (
          <ClickableId
            onClick={() =>
              addPages([
                {
                  coId: row.source_id as CoID<RawCoValue>,
                  name: row.source_id,
                },
              ])
            }
          >
            <Text mono small>
              {row.source_id}
            </Text>
          </ClickableId>
        ) : (
          <Text muted small>
            —
          </Text>
        ),
      sortable: true,
    },
    {
      id: "value",
      header: "Count",
      accessor: (row) => <Text small>{row.value}</Text>,
      sortable: true,
      sortFn: (a, b) => b.value - a.value,
    },
  ];

  const slowLoadTimesCount = metrics.tools.loadTimes.filter(
    (lt) => lt.loadTime > 200,
  ).length;

  return (
    <DashboardContainer>
      <MetricsSummaryRow>
        <MetricsSummaryColumn>
          <SectionHeading>CoValues by State</SectionHeading>
          <MetricsSummaryItems>
            <div>{metrics.cojson.available} available</div>
            <div>{metrics.cojson.loading} loading</div>
            <div>{metrics.cojson.unknown} unknown</div>
            <div>{metrics.cojson.unavailable} unavailable</div>
          </MetricsSummaryItems>
        </MetricsSummaryColumn>

        <MetricsSummaryColumn>
          <SectionHeading>Transport Metrics</SectionHeading>
          <MetricsSummaryItems>
            <div>
              <strong>In:</strong> {formatBytes(metrics.transport.ingress)}
            </div>
            <div>
              <strong>Out:</strong> {formatBytes(metrics.transport.egress)}
            </div>
          </MetricsSummaryItems>
        </MetricsSummaryColumn>
      </MetricsSummaryRow>

      <Accordion
        title={`Active Subscriptions (${metrics.tools.activeSubscriptions.length})`}
        storageKey="perf-active-subscriptions"
      >
        {metrics.tools.activeSubscriptions.length === 0 ? (
          <Text muted>No active subscriptions</Text>
        ) : (
          <DataTable
            columns={subscriptionColumns}
            data={metrics.tools.activeSubscriptions}
            pageSize={10}
            getRowKey={(row, index) => `${row.id}-${index}`}
            emptyMessage="No active subscriptions"
          />
        )}
      </Accordion>

      <Accordion
        title={`CoValues load times ${slowLoadTimesCount > 0 ? `(${slowLoadTimesCount} slow)` : ""}`}
        storageKey="perf-load-times"
      >
        {metrics.tools.loadTimes.length === 0 ? (
          <Text muted>No load time data available</Text>
        ) : (
          <DataTable
            columns={loadTimeColumns}
            data={metrics.tools.loadTimes}
            pageSize={10}
            initialSort={{ columnId: "loadTime", direction: "desc" }}
            getRowKey={(row, index) => `${row.id}-${index}`}
            emptyMessage="No load time data available"
          />
        )}
      </Accordion>
    </DashboardContainer>
  );
}

const DashboardContainer = styled("div")`
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  padding: 1rem 0;
`;

const SectionHeading = styled(Heading)`
  font-size: 1rem;
  text-align: left;
`;

const MetricsSummaryRow = styled("div")`
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1.5rem;
  justify-content: space-between;
`;

const MetricsSummaryColumn = styled("div")`
  display: flex;
  flex-direction: row;
  align-items: center;
  gap: 1rem;
`;

const MetricsSummaryItems = styled("div")`
  display: flex;
  flex-direction: column;
  gap: 0.5rem;

  @media (min-width: 724px) {
    flex-direction: row;
    gap: 1rem;
  }
`;

const ClickableId = styled("span")`
  cursor: pointer;
  color: var(--j-link-color);
  text-decoration: underline;

  &:hover {
    opacity: 0.8;
  }
`;
