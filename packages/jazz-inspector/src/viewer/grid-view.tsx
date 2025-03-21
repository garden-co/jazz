import { CoID, LocalNode, RawCoValue } from "cojson";
import { JsonObject } from "cojson";
import { Button } from "../ui/button.js";
import { ResolveIcon } from "./type-icon.js";
import { PageInfo, isCoId } from "./types.js";
import { CoMapPreview, ValueRenderer } from "./value-renderer.js";

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
    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
      {entries.map(([key, child], childIndex) => (
        <Button
          variant="plain"
          key={childIndex}
          className={`p-3 text-left rounded-lg overflow-hidden transition-colors ${
            isCoId(child)
              ? "border border-gray-200 shadow-sm hover:bg-gray-100/5"
              : "bg-gray-50  dark:bg-gray-925 cursor-default"
          }`}
          onClick={() =>
            isCoId(child) &&
            onNavigate([{ coId: child as CoID<RawCoValue>, name: key }])
          }
        >
          <h3 className="overflow-hidden text-ellipsis whitespace-nowrap">
            {isCoId(child) ? (
              <span className="font-medium flex justify-between">
                {key}

                <div className="py-1 px-2 text-sm bg-gray-100 rounded dark:bg-gray-900">
                  <ResolveIcon coId={child as CoID<RawCoValue>} node={node} />
                </div>
              </span>
            ) : (
              <span>{key}</span>
            )}
          </h3>
          <div className="mt-2 text-sm">
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
