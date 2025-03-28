import { CoID, LocalNode, RawCoValue } from "cojson";
import { JsonObject } from "cojson";
import { Button } from "../ui/button.js";
import { ResolveIcon } from "./type-icon.js";
import { PageInfo, isCoId } from "./types.js";
import { CoMapPreview, ValueRenderer } from "./value-renderer.js";

import { Badge } from "../ui/badge.js";
import { Text } from "../ui/text.js";
import { classNames } from "../utils.js";

export function GridView({
  data,
  onNavigate,
  node,
}: {
  data: JsonObject;
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
}) {
  const entries = Object.entries(data);

  return (
    <div
      className={classNames(
        "grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4",
      )}
    >
      {entries.map(([key, child], childIndex) => (
        <Button
          variant="plain"
          key={childIndex}
          className={classNames(
            `p-3 text-left rounded-lg overflow-hidden transition-colors ${
              isCoId(child)
                ? "border border-gray-200 shadow-sm hover:bg-gray-100/5"
                : "bg-gray-50  dark:bg-gray-925 cursor-default"
            }`,
          )}
          onClick={() =>
            isCoId(child) &&
            onNavigate([{ coId: child as CoID<RawCoValue>, name: key }])
          }
        >
          <h3
            className={classNames(
              "flex justify-between overflow-hidden text-ellipsis whitespace-nowrap",
            )}
          >
            {isCoId(child) ? (
              <>
                <Text strong>{key}</Text>

                <Badge>
                  <ResolveIcon coId={child as CoID<RawCoValue>} node={node} />
                </Badge>
              </>
            ) : (
              <Text strong>{key}</Text>
            )}
          </h3>
          <div className={classNames("mt-2 text-sm")}>
            {isCoId(child) ? (
              <CoMapPreview coId={child as CoID<RawCoValue>} node={node} />
            ) : (
              <ValueRenderer
                json={child}
                onCoIDClick={(coId) => {
                  onNavigate([{ coId, name: key }]);
                }}
              />
            )}
          </div>
        </Button>
      ))}
    </div>
  );
}
