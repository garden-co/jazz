import { useEffect, useState } from "react";
import { styled } from "goober";
import { CoID, RawCoValue } from "cojson";
import { Accordion } from "../ui/accordion";
import { DataTable, type ColumnDef } from "../ui/data-table";
import { Heading } from "../ui/heading";
import { Text } from "../ui/text";
import { useRouter } from "../router";
import { jazzMetricReader } from "../utils/instrumentation";
import {
  exportOTelMetrics,
  getLoadTimes,
  type OTelMetrics,
  type LoadTimeMetric,
} from "../utils/performances";

export default function Perf() {
  const [otelError, setOtelError] = useState<Error | null>(null);
  const [otelMetrics, setOtelMetrics] = useState<OTelMetrics | null>(null);
  const [loadTimes, setLoadTimes] = useState<LoadTimeMetric[]>([]);
  const [loadTimesResetTimestamp, setLoadTimesResetTimestamp] = useState<
    number | null
  >(null);
  const { addPages } = useRouter();

  useEffect(() => {
    async function fetchMetrics() {
      try {
        const metrics = await jazzMetricReader.collectMetrics();
        setOtelMetrics(exportOTelMetrics(metrics));
        setOtelError(null);
      } catch (error) {
        setOtelError(error as Error);
      }

      setLoadTimes(getLoadTimes());
    }

    const interval = setInterval(fetchMetrics, 1_000);
    fetchMetrics();
    return () => clearInterval(interval);
  }, []);

  const hasAnyData = otelMetrics !== null || loadTimes.length > 0;

  if (!hasAnyData && otelError) {
    return (
      <DashboardContainer>
        <Text>
          Error fetching metrics: {otelError.message}. Check the{" "}
          <a
            href="https://jazz.tools/docs/tooling-and-resources/inspector#performance-tab"
            target="_blank"
            rel="noopener noreferrer"
          >
            docs
          </a>
          .
        </Text>
      </DashboardContainer>
    );
  }

  if (!hasAnyData) {
    return (
      <DashboardContainer>
        <Text>Loading metrics...</Text>
      </DashboardContainer>
    );
  }

  const loadTimeColumns: ColumnDef<LoadTimeMetric>[] = [
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
      header: "Parent",
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
      id: "loadFrom",
      header: "Loaded from",
      accessor: (row) => row.loadFrom,
      sortable: false,
    },
  ];

  const subscriptionColumns: ColumnDef<
    OTelMetrics["activeSubscriptions"][number]
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

  const filteredLoadTimes = loadTimesResetTimestamp
    ? loadTimes.filter((lt) => lt.startTime >= loadTimesResetTimestamp)
    : loadTimes;

  const slowLoadTimesCount = filteredLoadTimes.filter(
    (lt) => lt.loadTime > 200,
  ).length;

  const activeSubscriptions = otelMetrics?.activeSubscriptions ?? [];

  return (
    <DashboardContainer>
      {otelError && (
        <ErrorBanner>
          <Text>OTel metrics error: {otelError.message}</Text>
        </ErrorBanner>
      )}

      <MetricsSummaryRow>
        <MetricsSummaryColumn>
          <SectionHeading>CoValues by State</SectionHeading>
          {otelMetrics ? (
            <MetricsSummaryItems>
              <div>{otelMetrics.cojson.available} available</div>
              <div>{otelMetrics.cojson.loading} loading</div>
              <div>{otelMetrics.cojson.unknown} unknown</div>
              <div>{otelMetrics.cojson.unavailable} unavailable</div>
            </MetricsSummaryItems>
          ) : (
            <Text muted small>
              Not available
            </Text>
          )}
        </MetricsSummaryColumn>

        <MetricsSummaryColumn>
          <SectionHeading>Transport Metrics</SectionHeading>
          {otelMetrics ? (
            <MetricsSummaryItems>
              <div>
                <strong>In:</strong>{" "}
                {formatBytes(otelMetrics.transport.ingress)}
              </div>
              <div>
                <strong>Out:</strong>{" "}
                {formatBytes(otelMetrics.transport.egress)}
              </div>
            </MetricsSummaryItems>
          ) : (
            <Text muted small>
              Not available
            </Text>
          )}
        </MetricsSummaryColumn>
      </MetricsSummaryRow>

      {otelError && (
        <Accordion
          title={`Active Subscriptions (${activeSubscriptions.length})`}
          storageKey="perf-active-subscriptions"
        >
          {activeSubscriptions.length === 0 ? (
            <Text muted>No active subscriptions</Text>
          ) : (
            <DataTable
              columns={subscriptionColumns}
              data={activeSubscriptions}
              pageSize={10}
              initialSort={{ columnId: "count", direction: "desc" }}
              getRowKey={(row, index) => `${row.id}-${index}`}
              emptyMessage="No active subscriptions"
            />
          )}
        </Accordion>
      )}

      <Accordion
        title={`CoValues load times ${slowLoadTimesCount > 0 ? `(${slowLoadTimesCount} slow)` : ""}`}
        storageKey="perf-load-times"
      >
        <AccordionToolbar>
          <ResetButton
            onClick={() => setLoadTimesResetTimestamp(Date.now())}
            title="Reset load times"
          >
            Reset
          </ResetButton>
        </AccordionToolbar>
        {filteredLoadTimes.length === 0 ? (
          <Text muted>No load time data available</Text>
        ) : (
          <DataTable
            columns={loadTimeColumns}
            data={filteredLoadTimes}
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

const ErrorBanner = styled("div")`
  padding: 0.75rem 1rem;
  background-color: var(--j-error-bg, rgba(239, 68, 68, 0.1));
  border: 1px solid var(--j-error-border, rgba(239, 68, 68, 0.3));
  border-radius: 4px;
  color: var(--j-error-color, #ef4444);
`;

const AccordionToolbar = styled("div")`
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 0.5rem;
  margin-bottom: 0.5rem;
`;

const ResetButton = styled("button")`
  padding: 0.25rem 0.5rem;
  font-size: 0.75rem;
  background-color: var(--j-button-bg, rgba(128, 128, 128, 0.2));
  border: 1px solid var(--j-button-border, rgba(128, 128, 128, 0.3));
  border-radius: 4px;
  color: var(--j-text-color, inherit);
  cursor: pointer;

  &:hover {
    background-color: var(--j-button-hover-bg, rgba(128, 128, 128, 0.3));
  }
`;
