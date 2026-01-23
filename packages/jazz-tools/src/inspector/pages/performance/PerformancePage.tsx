import { styled } from "goober";
import { type CSSProperties, useMemo, useState, useDeferredValue } from "react";
import { CoID, RawCoValue } from "cojson";
import { useRouter } from "../../router/context.js";
import { Timeline } from "./Timeline.js";
import { SubscriptionRow } from "./SubscriptionRow.js";
import { SubscriptionDetailPanel } from "./SubscriptionDetailPanel.js";
import { usePerformanceEntries } from "./usePerformanceEntries.js";
import type { SubscriptionEntry } from "./types.js";
import { SubscriptionScope } from "jazz-tools";

// ============================================================================
// Styled Components
// ============================================================================

const Container = styled("div")`
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  padding: 1rem;
  height: 100%;
  min-height: 0;
`;

const MainLayout = styled("div")`
  display: flex;
  flex: 1;
  min-height: 0;
  gap: 1rem;
`;

const ListPanel = styled("div")`
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
`;

const Grid = styled("div")`
  display: grid;
  grid-template-columns:
    minmax(100px, 150px)
    minmax(150px, 1fr)
    minmax(100px, 200px)
    80px;
  grid-template-rows: min-content;
  overflow-y: auto;
  overflow-x: hidden;
  flex: 1;
  min-height: 0;
  position: relative;
`;

const HeaderCell = styled("div")`
  padding: 0.5rem;
  font-size: 0.625rem;
  font-weight: 600;
  color: var(--j-neutral-500);
  border-bottom: 1px solid var(--j-border-color);
  text-transform: uppercase;
  letter-spacing: 0.05em;
`;

const EmptyState = styled("div")`
  text-align: center;
  padding: 2rem;
  color: var(--j-neutral-500);
  font-size: 0.875rem;
`;

// ============================================================================
// Component
// ============================================================================

export interface PerformancePageProps {
  onNavigate: () => void;
  style?: CSSProperties;
}

export function PerformancePage({ onNavigate, style }: PerformancePageProps) {
  const entries = usePerformanceEntries();
  const [selectedRow, setSelectedRow] = useState<string | null>(null);
  const [timeSelection, setTimeSelection] = useState<[number, number] | null>(
    null,
  );
  const deferredSelection = useDeferredValue(timeSelection);
  const { setPage } = useRouter();

  const selectRow = (uuid: string) => {
    setSelectedRow((prev) => (prev === uuid ? null : uuid));
  };

  const sortedEntries = useMemo(() => {
    return [...entries].sort((a, b) => a.startTime - b.startTime);
  }, [entries]);

  const overallTimeRange = useMemo(() => {
    if (entries.length === 0) return null;
    const now = performance.now();
    return {
      min: Math.min(...entries.map((e) => e.startTime)),
      max: Math.max(...entries.map((e) => e.endTime ?? now)),
    };
  }, [entries]);

  const filteredEntries = useMemo(() => {
    if (!deferredSelection) return sortedEntries;
    const [startTime, endTime] = deferredSelection;
    const now = performance.now();
    return sortedEntries.filter((entry) => {
      const entryEnd = entry.endTime ?? now;
      // Entry overlaps with selection if it starts before selection ends and ends after selection starts
      return entry.startTime <= endTime && entryEnd >= startTime;
    });
  }, [sortedEntries, deferredSelection]);

  const displayRange = deferredSelection
    ? { min: deferredSelection[0], max: deferredSelection[1] }
    : overallTimeRange;

  // Calculate bar position for each entry
  const getBarProps = (entry: SubscriptionEntry) => {
    const range = (displayRange?.max ?? 1) - (displayRange?.min ?? 0) || 1;
    const now = performance.now();

    const clampedStart = Math.max(entry.startTime, displayRange?.min ?? 0);
    const clampedEnd = Math.min(entry.endTime ?? now, displayRange?.max ?? now);

    const left = Math.max(
      0,
      ((clampedStart - (displayRange?.min ?? 0)) / range) * 100,
    );
    const width = Math.max(0, ((clampedEnd - clampedStart) / range) * 100);

    const color =
      entry.status === "pending"
        ? "var(--j-warning-color)"
        : entry.status === "error"
          ? "var(--j-error-color)"
          : "var(--j-success-color)";

    return {
      barLeft: `${left}%`,
      barWidth: width === 0 ? "1px" : `${width}%`,
      barColor: color,
    };
  };

  const handleNavigateToCoValue = (id: string) => {
    setPage(id as CoID<RawCoValue>);
    onNavigate();
  };

  if (!SubscriptionScope.isProfilingEnabled) {
    return (
      <Container style={style}>
        <EmptyState>Profiling is not enabled in production builds.</EmptyState>
      </Container>
    );
  }

  if (entries.length === 0) {
    return (
      <Container style={style}>
        <EmptyState>
          No subscriptions recorded yet. Interact with your app to see
          subscription performance data.
        </EmptyState>
      </Container>
    );
  }

  const selectedEntry = selectedRow
    ? filteredEntries.find((e) => e.uuid === selectedRow)
    : null;

  return (
    <Container style={style}>
      {overallTimeRange && (
        <Timeline
          entries={sortedEntries}
          timeRange={overallTimeRange}
          selection={timeSelection}
          onSelectionChange={setTimeSelection}
        />
      )}
      <MainLayout>
        <ListPanel>
          <Grid>
            <HeaderCell>Source</HeaderCell>
            <HeaderCell>CoValue</HeaderCell>
            <HeaderCell>Caller</HeaderCell>
            <HeaderCell>Duration</HeaderCell>
            {filteredEntries.map((entry) => (
              <SubscriptionRow
                key={entry.uuid}
                entry={entry}
                isSelected={selectedRow === entry.uuid}
                onSelect={() => selectRow(entry.uuid)}
                {...getBarProps(entry)}
              />
            ))}
          </Grid>
        </ListPanel>
        {selectedEntry && (
          <SubscriptionDetailPanel
            entry={selectedEntry}
            onNavigate={handleNavigateToCoValue}
            onClose={() => setSelectedRow(null)}
          />
        )}
      </MainLayout>
    </Container>
  );
}
