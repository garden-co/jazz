import { CoID, LocalNode, RawCoStream, RawCoValue } from "cojson";
import { useEffect, useState } from "react";
import { CoStreamView } from "./co-stream-view.js";
import { GridView } from "./grid-view.js";
import { TableView } from "./table-viewer.js";
import { TypeIcon } from "./type-icon.js";
import { PageInfo } from "./types.js";
import { useResolvedCoValue } from "./use-resolve-covalue.js";
import { AccountOrGroupPreview } from "./value-renderer.js";

type PageProps = {
  coId: CoID<RawCoValue>;
  node: LocalNode;
  name: string;
  onNavigate: (newPages: PageInfo[]) => void;
  onHeaderClick?: () => void;
  isTopLevel?: boolean;
  style: React.CSSProperties;
  className?: string;
};

export function Page({
  coId,
  node,
  name,
  onNavigate,
  onHeaderClick,
  style,
  className = "",
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
      style={style}
      className={
        className +
        " absolute z-10 inset-0 border rounded-xl shadow-lg p-6 w-full h-full bg-clip-padding"
      }
    >
      {!isTopLevel && (
        <div
          className="absolute left-0 right-0 top-0 h-10"
          aria-label="Back"
          onClick={() => {
            onHeaderClick?.();
          }}
          aria-hidden="true"
        ></div>
      )}
      <div className="flex justify-between items-center mb-4">
        <div className="flex flex-col gap-2">
          <h2 className="text-2xl font-bold flex flex-col items-start gap-1">
            <span>
              {name}
              {typeof snapshot === "object" && "name" in snapshot ? (
                <span className="text-gray-600 font-medium">
                  {" "}
                  {(snapshot as { name: string }).name}
                </span>
              ) : null}
            </span>
          </h2>
          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-700 font-medium py-0.5 px-1 -ml-0.5 rounded bg-gray-700/5 inline-block font-mono">
              {type && <TypeIcon type={type} extendedType={extendedType} />}
            </span>
            <span className="text-xs text-gray-700 font-medium py-0.5 px-1 -ml-0.5 rounded bg-gray-700/5 inline-block font-mono">
              {coId}
            </span>
          </div>
        </div>
      </div>
      <div className="overflow-auto max-h-[calc(100%-4rem)]">
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
          <div className="text-xs text-gray-500 mt-4">
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
