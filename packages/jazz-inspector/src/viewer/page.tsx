import { CoID, LocalNode, RawCoStream, RawCoValue } from "cojson";
import { useEffect, useState } from "react";
import { CoStreamView } from "./co-stream-view.tsx";
import { GridView } from "./grid-view.tsx";
import { TableView } from "./table-viewer.tsx";
import { TypeIcon } from "./type-icon.tsx";
import { PageInfo } from "./types.ts";
import { useResolvedCoValue } from "./use-resolve-covalue.ts";
import { AccountOrGroupPreview } from "./value-renderer.tsx";

type PageProps = {
  coId: CoID<RawCoValue>;
  node: LocalNode;
  name: string;
  onNavigate: (newPages: PageInfo[]) => void;
  onHeaderClick?: () => void;
  isTopLevel?: boolean;
  style: React.CSSProperties;
};

export function Page({
  coId,
  node,
  name,
  onNavigate,
  onHeaderClick,
  style,
  isTopLevel,
}: PageProps) {
  const { value, snapshot, type, extendedType } = useResolvedCoValue(
    coId,
    node,
  );
  const [viewMode, setViewMode] = useState<"grid" | "table">("grid");

  const supportsTableView = type === "colist" || extendedType === "record";

  // Automatically switch to table view if the page is a CoMap record
  useEffect(() => {
    if (supportsTableView) {
      setViewMode("table");
    }
  }, [supportsTableView]);

  if (snapshot === "unavailable") {
    return <div style={style}>Data unavailable</div>;
  }

  if (!snapshot) {
    return <div style={style}></div>;
  }

  return (
    <div
      style={{
        position: "absolute",
        inset: 0,
        backgroundColor: "white",
        borderWidth: "1px",
        borderColor: "rgba(0, 0, 0, 0.05)",
        borderRadius: "0.75rem",
        boxShadow:
          "0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05)",
        padding: "1.5rem",
        width: "100%",
        height: "100%",
        backgroundClip: "padding-box",
      }}
    >
      {!isTopLevel && (
        <div
          style={{
            position: "absolute",
            left: 0,
            right: 0,
            top: 0,
            height: "2.5rem",
          }}
          aria-label="Back"
          onClick={() => {
            onHeaderClick?.();
          }}
          aria-hidden="true"
        ></div>
      )}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: "1rem",
        }}
      >
        <div
          style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}
        >
          <h2
            style={{
              fontSize: "1.5rem",
              fontWeight: "700",
              display: "flex",
              alignItems: "flex-start",
              flexDirection: "column",
              gap: "0.25rem",
            }}
          >
            <span>
              {name}
              {typeof snapshot === "object" && "name" in snapshot ? (
                <span style={{ color: "rgb(75, 85, 99)", fontWeight: "500" }}>
                  {" "}
                  {(snapshot as { name: string }).name}
                </span>
              ) : null}
            </span>
          </h2>
          <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
            <span
              style={{
                fontSize: "0.75rem",
                color: "rgb(55, 65, 81)",
                fontWeight: "500",
                padding: "0.125rem 0.25rem",
                marginLeft: "-0.125rem",
                borderRadius: "0.25rem",
                backgroundColor: "rgba(55, 65, 81, 0.05)",
                display: "inline-block",
                fontFamily: "monospace",
              }}
            >
              {type && <TypeIcon type={type} extendedType={extendedType} />}
            </span>
            <span
              style={{
                fontSize: "0.75rem",
                color: "rgb(55, 65, 81)",
                fontWeight: "500",
                padding: "0.125rem 0.25rem",
                marginLeft: "-0.125rem",
                borderRadius: "0.25rem",
                backgroundColor: "rgba(55, 65, 81, 0.05)",
                display: "inline-block",
                fontFamily: "monospace",
              }}
            >
              {coId}
            </span>
          </div>
        </div>
      </div>
      <div style={{ overflow: "auto", maxHeight: "calc(100% - 4rem)" }}>
        {type === "costream" ? (
          <CoStreamView
            data={snapshot}
            onNavigate={onNavigate}
            node={node}
            value={value as RawCoStream}
          />
        ) : viewMode === "grid" ? (
          <GridView data={snapshot} onNavigate={onNavigate} node={node} />
        ) : (
          <TableView data={snapshot} node={node} onNavigate={onNavigate} />
        )}
        {extendedType !== "account" && extendedType !== "group" && (
          <div
            style={{
              fontSize: "0.75rem",
              color: "rgb(107, 114, 128)",
              marginTop: "1rem",
            }}
          >
            Owned by{" "}
            <AccountOrGroupPreview
              coId={value.group.id}
              node={node}
              showId
              onClick={() => {
                onNavigate([{ coId: value.group.id, name: "owner" }]);
              }}
            />
          </div>
        )}
      </div>
    </div>
  );
}
