import { styled } from "goober";
import {
  type CSSProperties,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { SubscriptionEntry } from "./types.js";
import { formatDuration } from "./helpers.js";

// ============================================================================
// Styled Components
// ============================================================================

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

// ============================================================================
// Component
// ============================================================================

export interface TimelineProps {
  entries: SubscriptionEntry[];
  timeRange: { min: number; max: number };
  selection: [number, number] | null;
  onSelectionChange: (selection: [number, number] | null) => void;
}

type DragMode = "creating" | "moving" | "resizing-left" | "resizing-right";

export function Timeline({
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

    const maxMarkers = 5;

    // Calculate minimum interval to have at most maxMarkers
    const minInterval = duration / maxMarkers;

    // Round up to a "nice" number (1, 2, 5, 10, 20, 50, 100, ...)
    const magnitude = Math.pow(10, Math.floor(Math.log10(minInterval)));
    const normalized = minInterval / magnitude;
    let niceMultiplier: number;
    if (normalized <= 1) niceMultiplier = 1;
    else if (normalized <= 2) niceMultiplier = 2;
    else if (normalized <= 5) niceMultiplier = 5;
    else niceMultiplier = 10;

    const interval = niceMultiplier * magnitude;

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

  // Pre-calculate lane assignments to avoid overlaps
  const laneAssignments = useMemo(() => {
    const now = performance.now();
    const barHeight = 3;
    const barGap = 1;
    const maxLanes = 8;

    // Sort entries by start time for lane assignment
    const sortedEntries = [...entries].sort(
      (a, b) => a.startTime - b.startTime,
    );

    // Track end times for each lane (up to maxLanes)
    const laneEndTimes: number[] = Array(maxLanes).fill(0);
    const assignments = new Map<string, number>();

    for (const entry of sortedEntries) {
      const entryEnd = entry.endTime ?? now;

      // Find the first lane where this entry fits (no overlap)
      let assignedLane = laneEndTimes.findIndex(
        (endTime) => entry.startTime >= endTime,
      );

      if (assignedLane === -1) {
        // All lanes are occupied, find the one that ends earliest
        const earliestEnd = Math.min(...laneEndTimes);
        assignedLane = laneEndTimes.indexOf(earliestEnd);
      }

      // Update the lane's end time
      laneEndTimes[assignedLane] = entryEnd;

      // Calculate top position
      assignments.set(entry.uuid, assignedLane * (barHeight + barGap));
    }

    return assignments;
  }, [entries]);

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

    const top = laneAssignments.get(entry.uuid) ?? 0;

    return {
      left: `${left}%`,
      width: `${width}%`,
      backgroundColor: color,
      top: `${top}px`,
      height: "3px",
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
