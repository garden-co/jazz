import { CoID, LocalNode, RawCoStream, RawCoValue } from "cojson";
import { useMemo } from "react";
import { Badge } from "../ui/badge.js";
import { Heading } from "../ui/heading.js";
import { Text } from "../ui/text.js";
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
          <Heading
            className={classNames("flex flex-col items-start gap-1 mb-4")}
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
          </Heading>
          <Badge>
            {type && <TypeIcon type={type} extendedType={extendedType} />}
          </Badge>
          <Badge>{coId}</Badge>
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
          <Text muted className={classNames("mt-4")}>
            Owned by{" "}
            <AccountOrGroupPreview
              coId={value.group.id}
              node={node}
              showId
              onClick={() => {
                onNavigate([{ coId: value.group.id, name: "owner" }]);
              }}
            />
          </Text>
        )}
      </div>
    </div>
  );
}
