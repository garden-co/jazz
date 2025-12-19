import { useEffect, useState } from "react";
import { styled } from "goober";
import { CoID, RawCoValue } from "cojson";
import { Accordion } from "../ui/accordion";
import { DataTable, type ColumnDef } from "../ui/data-table";
import { Heading } from "../ui/heading";
import { Text } from "../ui/text";
import { useRouter } from "../router";
import { jazzMetricReader } from "../utils/instrumentation";
import { exportMetrics, type PerfMetrics } from "../utils/performances";

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
      sortable: false,
    },
    {
      id: "loadFromStorage",
      header: "Load From Storage",
      accessor: (row) => formatTime(row.loadFromStorage ?? 0),
      sortable: true,
      sortFn: (a, b) => (a.loadFromStorage ?? 0) - (b.loadFromStorage ?? 0),
    },
    {
      id: "loadFromPeer",
      header: "Load From Peer",
      accessor: (row) => formatTime(row.loadFromPeer ?? 0),
      sortable: true,
      sortFn: (a, b) => (a.loadFromPeer ?? 0) - (b.loadFromPeer ?? 0),
    },
    {
      id: "transactionParsing",
      header: "Transaction Parsing",
      accessor: (row) => formatTime(row.transactionParsing ?? 0),
      sortable: true,
      sortFn: (a, b) =>
        (a.transactionParsing ?? 0) - (b.transactionParsing ?? 0),
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
      id: "sources",
      header: "Sources",
      accessor: (row) =>
        row.sources.map((source) => (
          <ClickableId
            key={source}
            onClick={() =>
              addPages([
                {
                  coId: source as CoID<RawCoValue>,
                  name: source,
                },
              ])
            }
          >
            <Text mono small>
              {source}
            </Text>
          </ClickableId>
        )),
      sortable: false,
    },
    {
      id: "count",
      header: "Count",
      accessor: (row) => <Text small>{row.count}</Text>,
      sortable: true,
      sortFn: (a, b) => a.count - b.count,
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
            initialSort={{ columnId: "count", direction: "desc" }}
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
