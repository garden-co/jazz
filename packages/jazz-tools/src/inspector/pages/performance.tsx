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

const RowWrapper = styled("div")`
  display: grid;
  grid-template-columns: subgrid;
  grid-column: 1 / -1;
  position: relative;
  cursor: pointer;

  &:hover,
  &:focus {
    background-color: var(--j-foreground);
    outline: none;
  }

  &:focus-visible {
    outline: 2px solid var(--j-primary-color);
    outline-offset: -2px;
  }

  &[data-expanded="true"] {
    background-color: var(--j-foreground);
  }
`;

const TimeBar = styled("div")`
  position: absolute;
  bottom: 0;
  height: 4px;
  transition: transform 0.15s ease;
  z-index: 1;
  container-type: inline-size;

  .row-wrapper:hover &, [data-expanded="true"] & {
    transform: scaleY(4);

    .time-label {
      opacity: 1;
    }
  }

  &[data-status="pending"] {
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

const TimeLabel = styled("span")`
  position: absolute;
  top: 50%;
  transform: translateY(-50%) scaleY(0.25);
  font-size: 0.5rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  color: white;
  white-space: nowrap;
  opacity: 0;
  transition: opacity 0.15s ease;
  pointer-events: none;
  left: 4px;

  @media (prefers-color-scheme: dark) {
    --time-label-overflow-color: black;
  }

  @container (max-width: 50px) {
    color: var(--time-label-overflow-color, inherit);
    left: 100%;
    margin-left: 4px;
  }

  [data-near-edge="true"] & {
    @container (max-width: 50px) {
      left: auto;
      right: 100%;
      margin-left: 0;
      margin-right: 4px;
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

const DetailPanel = styled("div")`
  width: 320px;
  flex-shrink: 0;
  padding: 0.75rem 1rem;
  background-color: var(--j-foreground);
  border: 1px solid var(--j-border-color);
  border-radius: var(--j-radius-sm);
  overflow-y: auto;
  position: relative;
`;

const CloseButton = styled("button")`
  position: absolute;
  top: 0.5rem;
  right: 0.5rem;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 18px;
  height: 18px;
  padding: 0;
  background: none;
  border: none;
  border-radius: var(--j-radius-sm);
  cursor: pointer;
  color: var(--j-neutral-500);

  &:hover {
    background-color: var(--j-background);
    color: var(--j-text-color);
  }

  &:focus-visible {
    outline: 2px solid var(--j-primary-color);
    outline-offset: -2px;
  }
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

const TimelineContainer = styled("div")`
  position: relative;
  display: flex;
  flex-direction: column;
  background-color: var(--j-foreground);
  border: 1px solid var(--j-border-color);
  border-radius: var(--j-radius-sm);
  overflow: hidden;
`;

const TimelineTrack = styled("div")`
  position: relative;
  height: 48px;
  background-color: var(--j-background);
  cursor: crosshair;
  user-select: none;
`;

const TimeMarker = styled("div")`
  position: absolute;
  font-size: 0.5rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  color: var(--j-text-color);
  padding: 2px 4px;
  white-space: nowrap;

  @media (prefers-color-scheme: dark) {
    color: var(--j-neutral-500);
  }

  &::after {
    content: "";
    position: absolute;
    left: 0;
    top: 100%;
    width: 1px;
    height: 32px;
    background-color: var(--j-border-color);
  }
`;

const TimelineBars = styled("div")`
  position: absolute;
  top: 16px;
  left: 0;
  right: 0;
  bottom: 0;
  pointer-events: none;
`;

const TimelineBar = styled("div")`
  position: absolute;
  height: 6px;
  border-radius: 1px;
  min-width: 2px;
`;

const TimelineSelection = styled("div")`
  position: absolute;
  top: 0;
  bottom: 0;
  background-color: var(--j-primary-color);
  opacity: 0.2;
  cursor: grab;
  pointer-events: auto;

  &:active {
    cursor: grabbing;
  }
`;

const TimelineSelectionHandle = styled("div")`
  position: absolute;
  top: 0;
  bottom: 0;
  width: 2px;
  background-color: var(--j-primary-color);
  cursor: ew-resize;
  pointer-events: auto;

  &::after {
    content: "";
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    width: 8px;
    height: 16px;
    background-color: var(--j-primary-color);
    border-radius: 2px;
  }
`;

const ClearSelectionButton = styled("button")`
  position: absolute;
  top: 4px;
  right: 4px;
  display: flex;
  align-items: center;
  gap: 4px;
  padding: 2px 6px;
  font-size: 0.5rem;
  background-color: var(--j-foreground);
  border: 1px solid var(--j-border-color);
  border-radius: var(--j-radius-sm);
  cursor: pointer;
  color: var(--j-neutral-500);
  z-index: 10;

  &:hover {
    background-color: var(--j-background);
    color: var(--j-text-color);
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
    const entriesByUuid = new Map<string, SubscriptionEntry>();

    const handlePerformanceEntries = (entries: PerformanceEntry[]) => {
      for (const mark of entries) {
        const detail = (mark as PerformanceMark)
          .detail as SubscriptionPerformanceDetail;

        if (detail?.type !== "jazz-subscription") continue;

        const prevEntry = entriesByUuid.get(detail.uuid);

        if (mark.entryType === "mark" && prevEntry) continue;

        entriesByUuid.set(detail.uuid, {
          uuid: detail.uuid,
          id: detail.id,
          source: detail.source,
          resolve: JSON.stringify(detail.resolve),
          status: detail.status,
          startTime: mark.startTime,
          callerStack: detail.callerStack ?? prevEntry?.callerStack,
          duration: mark.entryType === "mark" ? undefined : mark.duration,
          endTime: mark.startTime + mark.duration,
          errorType: detail.errorType,
        });
      }
    };

    handlePerformanceEntries(performance.getEntriesByType("mark"));
    handlePerformanceEntries(performance.getEntriesByType("measure"));

    setEntries(Array.from(entriesByUuid.values()));

    const observer = new PerformanceObserver((list) => {
      handlePerformanceEntries(list.getEntries());
      setEntries(Array.from(entriesByUuid.values()));
    });

    observer.observe({ entryTypes: ["mark", "measure"] });

    return () => observer.disconnect();
  }, []);

  return entries;
}

// ============================================================================
// Sub-components
// ============================================================================

interface TimelineProps {
  entries: SubscriptionEntry[];
  timeRange: { min: number; max: number };
  selection: [number, number] | null;
  onSelectionChange: (selection: [number, number] | null) => void;
}

type DragMode = "creating" | "moving" | "resizing-left" | "resizing-right";

function Timeline({
  entries,
  timeRange,
  selection,
  onSelectionChange,
}: TimelineProps) {
  const trackRef = useRef<HTMLDivElement>(null);
  const [dragMode, setDragMode] = useState<DragMode | null>(null);
  const [dragStartTime, setDragStartTime] = useState<number | null>(null);
  const [dragCurrentTime, setDragCurrentTime] = useState<number | null>(null);
  const [dragInitialSelection, setDragInitialSelection] = useState<
    [number, number] | null
  >(null);

  const duration = timeRange.max - timeRange.min;

  // Generate time markers
  const timeMarkers = useMemo(() => {
    const markers: { time: number; label: string; position: number }[] = [];
    if (duration <= 0) return markers;

    // Determine appropriate interval based on duration
    let interval: number;
    if (duration <= 100) interval = 10;
    else if (duration <= 500) interval = 50;
    else if (duration <= 1000) interval = 100;
    else if (duration <= 5000) interval = 500;
    else if (duration <= 10000) interval = 1000;
    else if (duration <= 50000) interval = 5000;
    else if (duration <= 100000) interval = 10000;
    else interval = 30000;

    const startMarker = Math.ceil(timeRange.min / interval) * interval;
    for (let time = startMarker; time <= timeRange.max; time += interval) {
      const position = ((time - timeRange.min) / duration) * 100;
      markers.push({
        time,
        label: formatDuration(time),
        position,
      });
    }
    return markers;
  }, [timeRange, duration]);

  const getTimeFromPosition = (clientX: number): number => {
    if (!trackRef.current) return 0;
    const rect = trackRef.current.getBoundingClientRect();
    const position = Math.max(
      0,
      Math.min(1, (clientX - rect.left) / rect.width),
    );
    return timeRange.min + position * duration;
  };

  const handleTrackMouseDown = (e: React.MouseEvent) => {
    const time = getTimeFromPosition(e.clientX);
    setDragMode("creating");
    setDragStartTime(time);
    setDragCurrentTime(time);
  };

  const handleSelectionMouseDown = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (!selection) return;
    const time = getTimeFromPosition(e.clientX);
    setDragMode("moving");
    setDragStartTime(time);
    setDragCurrentTime(time);
    setDragInitialSelection(selection);
  };

  const handleLeftHandleMouseDown = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (!selection) return;
    setDragMode("resizing-left");
    setDragStartTime(selection[0]);
    setDragCurrentTime(selection[0]);
    setDragInitialSelection(selection);
  };

  const handleRightHandleMouseDown = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (!selection) return;
    setDragMode("resizing-right");
    setDragStartTime(selection[1]);
    setDragCurrentTime(selection[1]);
    setDragInitialSelection(selection);
  };

  useEffect(() => {
    if (!dragMode) return;

    const handleMouseMove = (e: MouseEvent) => {
      const time = getTimeFromPosition(e.clientX);
      setDragCurrentTime(time);
    };

    const handleMouseUp = () => {
      if (
        dragMode === "creating" &&
        dragStartTime !== null &&
        dragCurrentTime !== null
      ) {
        const start = Math.min(dragStartTime, dragCurrentTime);
        const end = Math.max(dragStartTime, dragCurrentTime);
        if ((end - start) / duration > 0.01) {
          onSelectionChange([start, end]);
        }
      } else if (
        dragMode === "moving" &&
        dragInitialSelection &&
        dragStartTime !== null &&
        dragCurrentTime !== null
      ) {
        const delta = dragCurrentTime - dragStartTime;
        const selectionWidth =
          dragInitialSelection[1] - dragInitialSelection[0];
        let newStart = dragInitialSelection[0] + delta;
        let newEnd = dragInitialSelection[1] + delta;
        // Clamp to time range
        if (newStart < timeRange.min) {
          newStart = timeRange.min;
          newEnd = timeRange.min + selectionWidth;
        }
        if (newEnd > timeRange.max) {
          newEnd = timeRange.max;
          newStart = timeRange.max - selectionWidth;
        }
        onSelectionChange([newStart, newEnd]);
      } else if (
        dragMode === "resizing-left" &&
        dragInitialSelection &&
        dragCurrentTime !== null
      ) {
        const newStart = Math.min(
          dragCurrentTime,
          dragInitialSelection[1] - duration * 0.01,
        );
        onSelectionChange([
          Math.max(timeRange.min, newStart),
          dragInitialSelection[1],
        ]);
      } else if (
        dragMode === "resizing-right" &&
        dragInitialSelection &&
        dragCurrentTime !== null
      ) {
        const newEnd = Math.max(
          dragCurrentTime,
          dragInitialSelection[0] + duration * 0.01,
        );
        onSelectionChange([
          dragInitialSelection[0],
          Math.min(timeRange.max, newEnd),
        ]);
      }

      setDragMode(null);
      setDragStartTime(null);
      setDragCurrentTime(null);
      setDragInitialSelection(null);
    };

    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);
    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
  }, [
    dragMode,
    dragStartTime,
    dragCurrentTime,
    dragInitialSelection,
    duration,
    timeRange,
    onSelectionChange,
  ]);

  const getBarStyle = (entry: SubscriptionEntry): CSSProperties => {
    const now = performance.now();
    const start = entry.startTime;
    const end = entry.endTime ?? now;
    const left = ((start - timeRange.min) / duration) * 100;
    const width = Math.max(0.5, ((end - start) / duration) * 100);

    const color =
      entry.status === "pending"
        ? "var(--j-warning-color)"
        : entry.status === "error"
          ? "var(--j-error-color)"
          : "var(--j-success-color)";

    return {
      left: `${left}%`,
      width: `${width}%`,
      backgroundColor: color,
      top: `${4 + (entries.indexOf(entry) % 4) * 7}px`,
    };
  };

  // Calculate current selection during drag
  const currentSelection = useMemo((): [number, number] | null => {
    if (
      dragMode === "creating" &&
      dragStartTime !== null &&
      dragCurrentTime !== null
    ) {
      return [
        Math.min(dragStartTime, dragCurrentTime),
        Math.max(dragStartTime, dragCurrentTime),
      ];
    }
    if (
      dragMode === "moving" &&
      dragInitialSelection &&
      dragStartTime !== null &&
      dragCurrentTime !== null
    ) {
      const delta = dragCurrentTime - dragStartTime;
      const selectionWidth = dragInitialSelection[1] - dragInitialSelection[0];
      let newStart = dragInitialSelection[0] + delta;
      let newEnd = dragInitialSelection[1] + delta;
      if (newStart < timeRange.min) {
        newStart = timeRange.min;
        newEnd = timeRange.min + selectionWidth;
      }
      if (newEnd > timeRange.max) {
        newEnd = timeRange.max;
        newStart = timeRange.max - selectionWidth;
      }
      return [newStart, newEnd];
    }
    if (
      dragMode === "resizing-left" &&
      dragInitialSelection &&
      dragCurrentTime !== null
    ) {
      const newStart = Math.max(
        timeRange.min,
        Math.min(dragCurrentTime, dragInitialSelection[1] - duration * 0.01),
      );
      return [newStart, dragInitialSelection[1]];
    }
    if (
      dragMode === "resizing-right" &&
      dragInitialSelection &&
      dragCurrentTime !== null
    ) {
      const newEnd = Math.min(
        timeRange.max,
        Math.max(dragCurrentTime, dragInitialSelection[0] + duration * 0.01),
      );
      return [dragInitialSelection[0], newEnd];
    }
    return selection;
  }, [
    dragMode,
    dragStartTime,
    dragCurrentTime,
    dragInitialSelection,
    selection,
    timeRange,
    duration,
  ]);

  const selectionLeft = currentSelection
    ? ((currentSelection[0] - timeRange.min) / duration) * 100
    : 0;
  const selectionWidth = currentSelection
    ? ((currentSelection[1] - currentSelection[0]) / duration) * 100
    : 0;

  return (
    <TimelineContainer>
      <TimelineTrack ref={trackRef} onMouseDown={handleTrackMouseDown}>
        {timeMarkers.map((marker) => (
          <TimeMarker key={marker.time} style={{ left: `${marker.position}%` }}>
            {marker.label}
          </TimeMarker>
        ))}
        <TimelineBars>
          {entries.map((entry) => (
            <TimelineBar key={entry.uuid} style={getBarStyle(entry)} />
          ))}
        </TimelineBars>
        {currentSelection && (
          <>
            <TimelineSelection
              style={{
                left: `${selectionLeft}%`,
                width: `${selectionWidth}%`,
              }}
              onMouseDown={handleSelectionMouseDown}
            />
            {!dragMode && (
              <>
                <TimelineSelectionHandle
                  style={{ left: `${selectionLeft}%` }}
                  onMouseDown={handleLeftHandleMouseDown}
                />
                <TimelineSelectionHandle
                  style={{ left: `${selectionLeft + selectionWidth}%` }}
                  onMouseDown={handleRightHandleMouseDown}
                />
              </>
            )}
          </>
        )}
      </TimelineTrack>
      {currentSelection && !dragMode && (
        <ClearSelectionButton
          onClick={(e) => {
            e.stopPropagation();
            onSelectionChange(null);
          }}
        >
          Clear selection
        </ClearSelectionButton>
      )}
    </TimelineContainer>
  );
}

interface SubscriptionRowProps {
  entry: SubscriptionEntry;
  isSelected: boolean;
  onSelect: () => void;
  barLeft: string;
  barWidth: string;
  barColor: string;
}

function SubscriptionRow({
  entry,
  isSelected,
  onSelect,
  barLeft,
  barWidth,
  barColor,
}: SubscriptionRowProps) {
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      onSelect();
    }
  };

  return (
    <RowWrapper
      className="row-wrapper"
      data-expanded={isSelected}
      onClick={onSelect}
      onKeyDown={handleKeyDown}
      tabIndex={0}
      role="button"
      aria-label={`View details for ${entry.source} ${entry.id}`}
    >
      <Cell>
        <StatusBadge data-status={entry.status}>{entry.source}</StatusBadge>
      </Cell>
      <Cell>
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: "0.125rem",
          }}
        >
          <span>{entry.id}</span>
          <span style={{ color: "var(--j-neutral-500)" }}>{entry.resolve}</span>
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
      <TimeBar
        className="time-bar"
        data-status={entry.status}
        data-near-edge={parseFloat(barLeft) + parseFloat(barWidth) > 85}
        style={{
          left: barLeft,
          width: barWidth,
          backgroundColor: barColor,
        }}
      >
        <TimeLabel className="time-label">
          {formatDuration(entry.duration ?? 0)}
        </TimeLabel>
      </TimeBar>
    </RowWrapper>
  );
}

interface SubscriptionDetailPanelProps {
  entry: SubscriptionEntry;
  onNavigate: (id: string) => void;
  onClose: () => void;
}

function SubscriptionDetailPanel({
  entry,
  onNavigate,
  onClose,
}: SubscriptionDetailPanelProps) {
  return (
    <DetailPanel>
      <CloseButton onClick={onClose} aria-label="Close detail panel">
        <svg
          width="10"
          height="10"
          viewBox="0 0 14 14"
          fill="none"
          xmlns="http://www.w3.org/2000/svg"
        >
          <path
            d="M1 1L13 13M1 13L13 1"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
          />
        </svg>
      </CloseButton>
      <DetailsGrid>
        <DetailLabel>Source</DetailLabel>
        <DetailValue>
          <StatusBadge data-status={entry.status}>{entry.source}</StatusBadge>
        </DetailValue>

        <DetailLabel>CoValue</DetailLabel>
        <DetailValue>
          <a
            title="Click to navigate to CoValue"
            onClick={() => onNavigate(entry.id)}
            style={{ color: "var(--j-link-color)", cursor: "pointer" }}
          >
            {entry.id}
          </a>
        </DetailValue>

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
    </DetailPanel>
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
    const width = Math.max(0.5, ((clampedEnd - clampedStart) / range) * 100);

    const color =
      entry.status === "pending"
        ? "var(--j-warning-color)"
        : entry.status === "error"
          ? "var(--j-error-color)"
          : "var(--j-success-color)";

    return {
      barLeft: `${left}%`,
      barWidth: `${width}%`,
      barColor: color,
    };
  };

  const handleNavigateToCoValue = (id: string) => {
    setPage(id as CoID<RawCoValue>);
    onNavigate();
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
        {selectedEntry ? (
          <SubscriptionDetailPanel
            entry={selectedEntry}
            onNavigate={handleNavigateToCoValue}
            onClose={() => setSelectedRow(null)}
          />
        ) : null}
      </MainLayout>
    </Container>
  );
}
