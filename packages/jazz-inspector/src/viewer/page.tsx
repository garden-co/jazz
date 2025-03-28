import { CoID, LocalNode, RawCoStream, RawCoValue } from "cojson";
import { useMemo } from "react";
import { classNames } from "../utils.js";
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
  style?: React.CSSProperties;
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

  const viewMode = useMemo(() => {
    if (type === "colist" || extendedType === "record") {
      return "table";
    } else {
      return "grid";
    }
  }, [type, extendedType]);

  if (snapshot === "unavailable") {
    return <div style={style}>Data unavailable</div>;
  }

  if (!snapshot) {
    return <div style={style}></div>;
  }

  return (
    <div
      style={style}
      className={className + "absolute z-10 inset-0 w-full h-full px-3"}
    >
      {!isTopLevel && (
        <div
          className={classNames("absolute left-0 right-0 top-0 h-10")}
          aria-label="Back"
          onClick={() => {
            onHeaderClick?.();
          }}
          aria-hidden="true"
        ></div>
      )}
      <div className={classNames("flex justify-between items-center mb-4")}>
        <div className={classNames("flex items-center gap-3")}>
          <h2
            className={classNames(
              "text-lg font-medium flex flex-col items-start gap-1 text-stone-900 dark:text-white",
            )}
          >
            <span>
              {name}
              {typeof snapshot === "object" && "name" in snapshot ? (
                <span className={classNames("text-gray-600 font-medium")}>
                  {" "}
                  {(snapshot as { name: string }).name}
                </span>
              ) : null}
            </span>
          </h2>
          <span
            className={classNames(
              "text-sm text-gray-700 font-medium py-0.5 px-1 -ml-0.5 rounded bg-gray-700/5 inline-block font-mono",
            )}
          >
            {type && <TypeIcon type={type} extendedType={extendedType} />}
          </span>
          <span
            className={classNames(
              "text-sm text-gray-700 font-medium py-0.5 px-1 -ml-0.5 rounded bg-gray-700/5 inline-block font-mono",
            )}
          >
            {coId}
          </span>
        </div>
      </div>
      <div className={classNames("overflow-auto")}>
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
          <div className={classNames("text-sm text-gray-500 mt-4")}>
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
