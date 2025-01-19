import { CoID, JsonValue, LocalNode, RawCoValue } from "cojson";
import React, { useEffect, useState } from "react";
import { LinkIcon } from "../link-icon.tsx";
import {
  isBrowserImage,
  resolveCoValue,
  useResolvedCoValue,
} from "./use-resolve-covalue.ts";

// Is there a chance we can pass the actual CoValue here?
export function ValueRenderer({
  json,
  compact,
  onCoIDClick,
}: {
  json: JsonValue | undefined;
  compact?: boolean;
  onCoIDClick?: (childNode: CoID<RawCoValue>) => void;
}) {
  if (typeof json === "undefined" || json === undefined) {
    return <span style={{ color: "#9CA3AF" }}>undefined</span>;
  }

  if (json === null) {
    return <span style={{ color: "#9CA3AF" }}>null</span>;
  }

  if (typeof json === "string" && json.startsWith("co_")) {
    const linkStyle = onCoIDClick
      ? {
          color: "#3B82F6",
          cursor: "pointer",
          display: "inline-flex",
          gap: "0.25rem",
          alignItems: "center",
        }
      : {
          display: "inline-flex",
          gap: "0.25rem",
          alignItems: "center",
        };

    return (
      <span
        style={linkStyle}
        onClick={() => {
          onCoIDClick?.(json as CoID<RawCoValue>);
        }}
      >
        {json}
        {onCoIDClick && <LinkIcon />}
      </span>
    );
  }

  if (typeof json === "string") {
    return (
      <span style={{ color: "#064E3B", fontFamily: "monospace" }}>{json}</span>
    );
  }

  if (typeof json === "number") {
    return <span style={{ color: "#A855F7" }}>{json}</span>;
  }

  if (typeof json === "boolean") {
    const booleanStyle = {
      color: json ? "#15803D" : "#B45309",
      backgroundColor: json
        ? "rgba(34, 197, 94, 0.05)"
        : "rgba(245, 158, 11, 0.05)",
      fontFamily: "monospace",
      display: "inline-block",
      padding: "0.125rem 0.25rem",
      borderRadius: "0.25rem",
    };

    return <span style={booleanStyle}>{json.toString()}</span>;
  }

  if (Array.isArray(json)) {
    return (
      <span title={JSON.stringify(json)}>
        Array <span style={{ color: "#6B7280" }}>({json.length})</span>
      </span>
    );
  }

  if (typeof json === "object") {
    return (
      <span
        title={JSON.stringify(json, null, 2)}
        style={{
          display: "inline-block",
          maxWidth: "16rem",
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        {compact ? (
          <span>
            Object{" "}
            <span style={{ color: "#6B7280" }}>
              ({Object.keys(json).length})
            </span>
          </span>
        ) : (
          JSON.stringify(json, null, 2)
        )}
      </span>
    );
  }

  return <span>{String(json)}</span>;
}

export const CoMapPreview = ({
  coId,
  node,
  limit = 6,
}: {
  coId: CoID<RawCoValue>;
  node: LocalNode;
  limit?: number;
}) => {
  const { value, snapshot, type, extendedType } = useResolvedCoValue(
    coId,
    node,
  );

  if (!snapshot) {
    return (
      <div
        style={{
          borderRadius: "0.25rem",
          backgroundColor: "#F3F4F6",
          animation: "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite",
          whiteSpace: "pre",
          width: "6rem",
        }}
      >
        {" "}
      </div>
    );
  }

  if (snapshot === "unavailable" && !value) {
    return <div style={{ color: "#6B7280" }}>Unavailable</div>;
  }

  if (extendedType === "image" && isBrowserImage(snapshot)) {
    return (
      <div>
        <img
          src={snapshot.placeholderDataURL}
          style={{
            width: "2rem",
            height: "2rem",
            border: "2px solid white",
            boxShadow:
              "0 1px 3px 0 rgb(0 0 0 / 0.1), 0 1px 2px -1px rgb(0 0 0 / 0.1)",
            margin: "0.5rem 0",
          }}
        />
        <span style={{ color: "#6B7280", fontSize: "0.875rem" }}>
          {snapshot.originalSize[0]} x {snapshot.originalSize[1]}
        </span>
      </div>
    );
  }

  if (extendedType === "record") {
    return (
      <div>
        Record{" "}
        <span style={{ color: "#6B7280" }}>
          ({Object.keys(snapshot).length})
        </span>
      </div>
    );
  }

  if (type === "colist") {
    return (
      <div>
        List{" "}
        <span style={{ color: "#6B7280" }}>
          ({(snapshot as unknown as []).length})
        </span>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "auto 1fr",
          gap: "0.5rem",
        }}
      >
        {Object.entries(snapshot)
          .slice(0, limit)
          .map(([key, value]) => (
            <React.Fragment key={key}>
              <span style={{ fontWeight: "bold" }}>{key}: </span>
              <span>
                <ValueRenderer json={value} />
              </span>
            </React.Fragment>
          ))}
      </div>
      {Object.entries(snapshot).length > limit && (
        <div
          style={{ textAlign: "left", fontSize: "0.875rem", color: "#6B7280" }}
        >
          {Object.entries(snapshot).length - limit} more
        </div>
      )}
    </div>
  );
};

export function AccountOrGroupPreview({
  coId,
  node,
  showId = false,
  onClick,
}: {
  coId: CoID<RawCoValue>;
  node: LocalNode;
  showId?: boolean;
  onClick?: (name?: string) => void;
}) {
  const { snapshot, extendedType } = useResolvedCoValue(coId, node);
  const [name, setName] = useState<string | null>(null);

  useEffect(() => {
    if (extendedType === "account") {
      resolveCoValue(
        (snapshot as unknown as { profile: CoID<RawCoValue> }).profile,
        node,
      ).then(({ snapshot }) => {
        if (
          typeof snapshot === "object" &&
          "name" in snapshot &&
          typeof snapshot.name === "string"
        ) {
          setName(snapshot.name);
        }
      });
    }
  }, [snapshot, node, extendedType]);

  if (!snapshot) return <span>Loading...</span>;
  if (extendedType !== "account" && extendedType !== "group") {
    return <span>CoID is not an account or group</span>;
  }

  const displayName = extendedType === "account" ? name || "Account" : "Group";
  const displayText = showId ? `${displayName} (${coId})` : displayName;

  const props = onClick
    ? {
        onClick: () => onClick(displayName),
        style: {
          color: "#3B82F6",
          cursor: "pointer",
          textDecoration: "underline",
        },
      }
    : {
        style: { color: "#6B7280" },
      };

  return <span {...props}>{displayText}</span>;
}
