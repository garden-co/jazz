import { styled } from "goober";
import {
  type CSSProperties,
  useEffect,
  useRef,
  useMemo,
  useState,
  useDeferredValue,
} from "react";
import { SubscriptionPerformanceDetail } from "jazz-tools";
import { useRouter } from "../router/context.js";
import { CoID, RawCoValue } from "cojson";

// ============================================================================
// Types
// ============================================================================

interface SubscriptionEntry {
  uuid: string;
  id: string;
  source: string;
  resolve: string;
  status: "pending" | "loaded" | "error";
  startTime: number;
  endTime?: number;
  duration?: number;
  errorType?: string;
  callerStack?: string;
}

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

const Grid = styled("div")`
  display: grid;
  grid-template-columns:
    minmax(100px, 150px)
    minmax(150px, 1fr)
    minmax(100px, 200px)
    80px;
  grid-template-rows: min-content;
  overflow: auto;
  flex: 1;
  min-height: 0;
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

const RowWrapper = styled("div")`
  display: grid;
  grid-template-columns: subgrid;
  grid-column: 1 / -1;
  position: relative;
  cursor: pointer;

  &:hover {
    background-color: var(--j-foreground);
  }

  &[data-expanded="true"] {
    background-color: var(--j-foreground);
  }

  &::after {
    content: "";
    position: absolute;
    bottom: 0;
    height: 4px;
    border-radius: 2px;
    left: var(--bar-left);
    width: var(--bar-width);
    background-color: var(--bar-color);
  }

  &[data-status="pending"]::after {
    animation: pulse 1.5s ease-in-out infinite;
  }

  @keyframes pulse {
    0%,
    100% {
      opacity: 1;
    }
    50% {
      opacity: 0.5;
    }
  }
`;

const Cell = styled("div")`
  padding: 0.5rem;
  padding-bottom: 0.75rem;
  font-size: 0.625rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  border-bottom: 1px solid var(--j-border-color);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;

  &[data-clickable="true"] {
    cursor: pointer;
    color: var(--j-link-color);
    &:hover {
      text-decoration: underline;
    }
  }
`;

const StatusBadge = styled("span")`
  display: inline-flex;
  align-items: center;
  gap: 0.25rem;
  padding: 0.125rem 0.375rem;
  border-radius: 0.25rem;
  font-size: 0.625rem;

  &[data-status="pending"] {
    background-color: var(--j-warning-bg);
    color: var(--j-warning-color);
  }
  &[data-status="loaded"] {
    background-color: var(--j-success-bg);
    color: var(--j-success-color);
  }
  &[data-status="error"] {
    background-color: var(--j-error-bg);
    color: var(--j-error-color);
  }
`;

const ExpandIcon = styled("span")`
  display: inline-block;
  transition: transform 0.15s ease;

  &[data-expanded="true"] {
    transform: rotate(90deg);
  }
`;

const ExpandedDetails = styled("div")`
  grid-column: 1 / -1;
  padding: 0.75rem 1rem;
  background-color: var(--j-foreground);
  border-bottom: 1px solid var(--j-border-color);
`;

const DetailsGrid = styled("div")`
  display: grid;
  grid-template-columns: auto 1fr;
  gap: 0.5rem 1rem;
  font-size: 0.625rem;
`;

const DetailLabel = styled("span")`
  color: var(--j-neutral-500);
  font-weight: 500;
`;

const DetailValue = styled("span")`
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  color: var(--j-text-color);
`;

const Pre = styled("pre")`
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.625rem;
  color: var(--j-text-color);
  margin: 0;
  white-space: pre-wrap;
  word-break: break-all;
  max-height: 200px;
  overflow-y: auto;
  background-color: var(--j-background);
  padding: 0.5rem;
  border-radius: var(--j-radius-sm);
`;

const SliderContainer = styled("div")`
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
`;

const SliderHeader = styled("div")`
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 0.625rem;
  color: var(--j-neutral-500);
`;

const SliderTrackWrapper = styled("div")`
  position: relative;
  height: 24px;
  display: flex;
  align-items: center;
`;

const SliderTrack = styled("div")`
  position: absolute;
  width: 100%;
  height: 4px;
  background-color: var(--j-neutral-200);
  border-radius: 2px;

  @media (prefers-color-scheme: dark) {
    background-color: var(--j-neutral-700);
  }
`;

const SliderRange = styled("div")`
  position: absolute;
  height: 4px;
  background-color: var(--j-primary-color);
  border-radius: 2px;
`;

const SliderInput = styled("input")`
  position: absolute;
  width: 100%;
  height: 4px;
  appearance: none;
  background: transparent;
  pointer-events: none;
  margin: 0;

  &::-webkit-slider-thumb {
    appearance: none;
    width: 12px;
    height: 12px;
    background-color: var(--j-primary-color);
    border-radius: 50%;
    cursor: pointer;
    pointer-events: auto;
    border: 2px solid white;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.2);
  }

  &::-moz-range-thumb {
    width: 12px;
    height: 12px;
    background-color: var(--j-primary-color);
    border-radius: 50%;
    cursor: pointer;
    pointer-events: auto;
    border: 2px solid white;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.2);
  }
`;

const EmptyState = styled("div")`
  text-align: center;
  padding: 2rem;
  color: var(--j-neutral-500);
  font-size: 0.875rem;
`;

const PendingDot = styled("span")`
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background-color: var(--j-warning-color);
  animation: pendingPulse 1.5s ease-in-out infinite;

  @keyframes pendingPulse {
    0%,
    100% {
      opacity: 1;
    }
    50% {
      opacity: 0.4;
    }
  }
`;

// ============================================================================
// Helper Functions
// ============================================================================

function formatTime(startTime: number): string {
  const date = new Date(performance.timeOrigin + startTime);
  return date.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  });
}

function formatDuration(duration: number): string {
  if (duration < 1) {
    return `${(duration * 1000).toFixed(0)}μs`;
  }
  if (duration < 1000) {
    return `${duration.toFixed(2)}ms`;
  }
  return `${(duration / 1000).toFixed(2)}s`;
}

function getCallerLocation(stack: string | undefined): string | undefined {
  if (!stack) return undefined;

  const lines = stack.split("\n").slice(2, 15);

  const normalizeLine = (line: string) =>
    line.replace(/https?:\/\/[^/\s)]+/g, "");

  const userFrame = lines.find(
    (line) =>
      !line.includes("node_modules") &&
      !line.includes("useCoValueSubscription") &&
      !line.includes("useCoState") &&
      !line.includes("useAccount") &&
      !line.includes("useSuspenseCoState") &&
      !line.includes("useSuspenseAccount") &&
      !line.includes("jazz-tools") &&
      !line.includes("trackLoadingPerformance"),
  );

  if (userFrame) {
    const cleanedFrame = normalizeLine(userFrame).trim();
    const match = cleanedFrame.match(/\(?([^)]+:\d+:\d+)\)?$/);
    if (match) {
      return match[1];
    }
    return cleanedFrame;
  }

  return lines[0] ? normalizeLine(lines[0]).trim() : undefined;
}

function getCallerStack(stack: string | undefined): string | undefined {
  if (!stack) return undefined;

  const lines = stack.split("\n").slice(2, 15);

  return lines
    .filter(
      (line) =>
        !line.includes("Error:") &&
        !line.includes("renderWithHooks") &&
        !line.includes("react-stack-bottom-frame"),
    )
    .map((line) => line.replace(/https?:\/\/[^/\s)]+/g, "").trim())
    .reverse()
    .join("\n");
}

// ============================================================================
// Custom Hooks
// ============================================================================

function usePerformanceEntries(): SubscriptionEntry[] {
  const [entries, setEntries] = useState<SubscriptionEntry[]>([]);

  useEffect(() => {
    // Collect existing marks on mount
    const existingMarks = performance
      .getEntriesByType("mark")
      .filter((m) => m.name.startsWith("jazz.subscription.start:"));

    const initialEntries: SubscriptionEntry[] = existingMarks.map((mark) => {
      const detail = (mark as PerformanceMark)
        .detail as SubscriptionPerformanceDetail;
      const uuid = mark.name.replace("jazz.subscription.start:", "");

      const measure = performance.getEntriesByName(
        `jazz.subscription:${uuid}`,
      )[0] as PerformanceMeasure | undefined;

      if (measure) {
        const measureDetail = measure.detail as SubscriptionPerformanceDetail;
        return {
          uuid,
          id: detail.id,
          source: detail.source,
          resolve: JSON.stringify(detail.resolve),
          status: measureDetail.status,
          startTime: mark.startTime,
          endTime: mark.startTime + measure.duration,
          duration: measure.duration,
          errorType: measureDetail.errorType,
          callerStack: detail.callerStack,
        };
      }

      return {
        uuid,
        id: detail.id,
        source: detail.source,
        resolve: JSON.stringify(detail.resolve),
        status: "pending" as const,
        startTime: mark.startTime,
        callerStack: detail.callerStack,
      };
    });

    setEntries(initialEntries);

    if (typeof PerformanceObserver === "undefined") {
      return;
    }

    const observer = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        if (entry.name.startsWith("jazz.subscription.start:")) {
          const detail = (entry as PerformanceMark)
            .detail as SubscriptionPerformanceDetail;
          setEntries((prev) => [
            ...prev,
            {
              uuid: detail.uuid,
              id: detail.id,
              source: detail.source,
              resolve: JSON.stringify(detail.resolve),
              status: "pending",
              startTime: entry.startTime,
              callerStack: detail.callerStack,
            },
          ]);
        } else if (
          entry.entryType === "measure" &&
          entry.name.startsWith("jazz.subscription:")
        ) {
          const uuid = entry.name.replace("jazz.subscription:", "");
          const detail = (entry as PerformanceMeasure)
            .detail as SubscriptionPerformanceDetail;

          setEntries((prev) =>
            prev.map((e) =>
              e.uuid === uuid
                ? {
                    ...e,
                    status: detail.status,
                    endTime: entry.startTime + entry.duration,
                    duration: entry.duration,
                    errorType: detail.errorType,
                  }
                : e,
            ),
          );
        }
      }
    });

    observer.observe({ entryTypes: ["mark", "measure"] });

    return () => observer.disconnect();
  }, []);

  return entries;
}

// ============================================================================
// Sub-components
// ============================================================================

interface TimeRangeSliderProps {
  value: [number, number];
  onChange: (value: [number, number]) => void;
  startLabel: string;
  endLabel: string;
  totalCount: number;
  filteredCount: number;
}

function TimeRangeSlider({
  value,
  onChange,
  startLabel,
  endLabel,
  totalCount,
  filteredCount,
}: TimeRangeSliderProps) {
  const maxIndex = Math.max(totalCount - 1, 0);
  const isDisabled = totalCount <= 1;

  const handleMinChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = Number(e.target.value);
    onChange([Math.min(newValue, value[1] - 1), value[1]]);
  };

  const handleMaxChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = Number(e.target.value);
    onChange([value[0], Math.max(newValue, value[0] + 1)]);
  };

  const sliderRange = maxIndex || 1;
  const sliderLeft = (value[0] / sliderRange) * 100;
  const sliderWidth = ((value[1] - value[0]) / sliderRange) * 100;

  return (
    <SliderContainer>
      <SliderHeader>
        <span>{startLabel}</span>
        <span>
          Showing {filteredCount} of {totalCount} subscriptions
        </span>
        <span>{endLabel}</span>
      </SliderHeader>
      <SliderTrackWrapper>
        <SliderTrack />
        <SliderRange
          style={{ left: `${sliderLeft}%`, width: `${sliderWidth}%` }}
        />
        <SliderInput
          type="range"
          min={0}
          max={maxIndex}
          value={value[0]}
          onChange={handleMinChange}
          disabled={isDisabled}
        />
        <SliderInput
          type="range"
          min={0}
          max={maxIndex}
          value={value[1]}
          onChange={handleMaxChange}
          disabled={isDisabled}
        />
      </SliderTrackWrapper>
    </SliderContainer>
  );
}

interface SubscriptionRowProps {
  entry: SubscriptionEntry;
  isExpanded: boolean;
  onToggle: () => void;
  onNavigate: (id: string) => void;
  barStyle: CSSProperties;
}

function SubscriptionRow({
  entry,
  isExpanded,
  onToggle,
  onNavigate,
  barStyle,
}: SubscriptionRowProps) {
  return (
    <>
      <RowWrapper
        data-expanded={isExpanded}
        data-status={entry.status}
        style={barStyle}
        onClick={onToggle}
      >
        <Cell>
          <StatusBadge data-status={entry.status}>
            <ExpandIcon data-expanded={isExpanded}>▶</ExpandIcon>
            {entry.source}
          </StatusBadge>
        </Cell>
        <Cell>
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              gap: "0.125rem",
            }}
          >
            <span
              data-clickable="true"
              onClick={(e) => {
                e.stopPropagation();
                onNavigate(entry.id);
              }}
              style={{ color: "var(--j-link-color)", cursor: "pointer" }}
            >
              {entry.id}
            </span>
            <span style={{ color: "var(--j-neutral-500)" }}>
              {entry.resolve}
            </span>
          </div>
        </Cell>
        <Cell>{getCallerLocation(entry.callerStack) ?? "-"}</Cell>
        <Cell>
          {entry.duration !== undefined ? (
            formatDuration(entry.duration)
          ) : (
            <PendingDot />
          )}
        </Cell>
      </RowWrapper>
      {isExpanded && (
        <ExpandedDetails>
          <DetailsGrid>
            <DetailLabel>Time</DetailLabel>
            <DetailValue>
              {formatTime(entry.startTime)} -{" "}
              {entry.duration !== undefined
                ? formatTime(entry.startTime + entry.duration)
                : "Pending..."}
            </DetailValue>

            <DetailLabel>Duration</DetailLabel>
            <DetailValue>
              {entry.duration !== undefined
                ? formatDuration(entry.duration)
                : "Pending..."}
            </DetailValue>

            <DetailLabel>Resolve Query</DetailLabel>
            <Pre>{JSON.stringify(JSON.parse(entry.resolve), null, 2)}</Pre>

            <DetailLabel>Stack Trace</DetailLabel>
            <Pre>
              {getCallerStack(entry.callerStack) ?? "No stack trace available"}
            </Pre>
          </DetailsGrid>
        </ExpandedDetails>
      )}
    </>
  );
}

// ============================================================================
// Main Component
// ============================================================================

interface PerformancePageProps {
  onNavigate: () => void;
  style?: CSSProperties;
}

export function PerformancePage({ onNavigate, style }: PerformancePageProps) {
  const entries = usePerformanceEntries();
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
  const maxRange: [number, number] = [0, entries.length - 1];
  const [subscriptionRange, setSubscriptionRange] = useState<
    [number, number] | null
  >(null);
  const deferredRange = useDeferredValue(subscriptionRange ?? maxRange);
  const { setPage } = useRouter();

  const toggleRow = (uuid: string) => {
    setExpandedRows((prev) => {
      const next = new Set(prev);
      if (next.has(uuid)) {
        next.delete(uuid);
      } else {
        next.add(uuid);
      }
      return next;
    });
  };

  // Sort entries by start time (newest first)
  const sortedEntries = useMemo(
    () => [...entries].sort((a, b) => b.startTime - a.startTime),
    [entries],
  );

  const filteredEntries = useMemo(() => {
    const [startIndex, endIndex] = deferredRange;
    return sortedEntries.slice(startIndex, endIndex + 1);
  }, [sortedEntries, deferredRange]);

  const overallTimeRange = useMemo(() => {
    if (entries.length === 0) return null;
    const now = performance.now();
    return {
      min: Math.min(...entries.map((e) => e.startTime)),
      max: Math.max(...entries.map((e) => e.endTime ?? now)),
    };
  }, [entries]);

  const selectedTimeRange = useMemo(() => {
    if (filteredEntries.length === 0) return null;
    const now = performance.now();
    return {
      min: Math.min(...filteredEntries.map((e) => e.startTime)),
      max: Math.max(...filteredEntries.map((e) => e.endTime ?? now)),
    };
  }, [filteredEntries]);

  const displayRange = selectedTimeRange ?? overallTimeRange;

  // Calculate bar position for each entry
  const getBarStyle = (entry: SubscriptionEntry): CSSProperties => {
    const range = (displayRange?.max ?? 1) - (displayRange?.min ?? 0) || 1;
    const now = performance.now();

    const clampedStart = Math.max(entry.startTime, displayRange?.min ?? 0);
    const clampedEnd = Math.min(entry.endTime ?? now, displayRange?.max ?? now);

    const left = Math.max(
      0,
      ((clampedStart - (displayRange?.min ?? 0)) / range) * 100,
    );
    const width = Math.max(0.5, ((clampedEnd - clampedStart) / range) * 100);

    const color =
      entry.status === "pending"
        ? "var(--j-warning-color)"
        : entry.status === "error"
          ? "var(--j-error-color)"
          : "var(--j-success-color)";

    return {
      "--bar-left": `${left}%`,
      "--bar-width": `${width}%`,
      "--bar-color": color,
    } as CSSProperties;
  };

  const handleNavigateToCoValue = (id: string) => {
    setPage(id as CoID<RawCoValue>);
    onNavigate();
  };

  const handleRangeChange = (value: [number, number]) => {
    setSubscriptionRange(value);
  };

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

  return (
    <Container style={style}>
      <TimeRangeSlider
        value={subscriptionRange ?? maxRange}
        onChange={handleRangeChange}
        startLabel={displayRange ? formatTime(displayRange.min) : "--"}
        endLabel={displayRange ? formatTime(displayRange.max) : "--"}
        totalCount={entries.length}
        filteredCount={filteredEntries.length}
      />
      <Grid>
        <HeaderCell>Source</HeaderCell>
        <HeaderCell>CoValue</HeaderCell>
        <HeaderCell>Caller</HeaderCell>
        <HeaderCell>Duration</HeaderCell>
        {filteredEntries.map((entry) => (
          <SubscriptionRow
            key={entry.uuid}
            entry={entry}
            isExpanded={expandedRows.has(entry.uuid)}
            onToggle={() => toggleRow(entry.uuid)}
            onNavigate={handleNavigateToCoValue}
            barStyle={getBarStyle(entry)}
          />
        ))}
      </Grid>
    </Container>
  );
}
