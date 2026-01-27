import { styled } from "goober";
import type { SubscriptionEntry } from "./types.js";
import { formatDuration, getCallerLocation } from "./helpers.js";

// ============================================================================
// Styled Components
// ============================================================================

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
  --time-label-overflow-color: black;

  @media (prefers-color-scheme: dark) {
    --time-label-overflow-color: white;
  }

  @container (max-width: 50px) {
    color: var(--time-label-overflow-color);
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
// Component
// ============================================================================

export interface SubscriptionRowProps {
  entry: SubscriptionEntry;
  isSelected: boolean;
  onSelect: () => void;
  barLeft: string;
  barWidth: string;
  barColor: string;
}

export function SubscriptionRow({
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
          entry.duration === 0 ? (
            "⚡ cached"
          ) : (
            formatDuration(entry.duration)
          )
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
          {entry.duration === 0
            ? "⚡ cached"
            : entry.duration
              ? formatDuration(entry.duration)
              : "-"}
        </TimeLabel>
      </TimeBar>
    </RowWrapper>
  );
}
