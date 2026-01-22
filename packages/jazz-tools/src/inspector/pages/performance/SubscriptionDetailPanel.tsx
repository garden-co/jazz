import { styled } from "goober";
import type { SubscriptionEntry } from "./types.js";
import { formatTime, formatDuration, getCallerStack } from "./helpers.js";

// ============================================================================
// Styled Components
// ============================================================================

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

// ============================================================================
// Component
// ============================================================================

export interface SubscriptionDetailPanelProps {
  entry: SubscriptionEntry;
  onNavigate: (id: string) => void;
  onClose: () => void;
}

export function SubscriptionDetailPanel({
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
          <button
            title="Click to navigate to CoValue"
            onClick={() => onNavigate(entry.id)}
            style={{
              color: "var(--j-link-color)",
              cursor: "pointer",
              background: "none",
              border: "none",
              padding: 0,
              font: "inherit",
            }}
          >
            {entry.id}
          </button>
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
