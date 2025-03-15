import { CoID, LocalNode, RawCoValue } from "cojson";
import type { JsonObject } from "cojson";
import { useMemo, useState } from "react";
import { LinkIcon } from "../link-icon.js";
import { PageInfo } from "./types.js";
import { useResolvedCoValues } from "./use-resolve-covalue.js";
import { ValueRenderer } from "./value-renderer.js";

export function TableView({
  data,
  node,
  onNavigate,
}: {
  data: JsonObject;
  node: LocalNode;
  onNavigate: (pages: PageInfo[]) => void;
}) {
  const [visibleRowsCount, setVisibleRowsCount] = useState(10);
  const [coIdArray, visibleRows] = useMemo(() => {
    const coIdArray = Array.isArray(data)
      ? data
      : Object.values(data).every(
            (k) => typeof k === "string" && k.startsWith("co_"),
          )
        ? Object.values(data).map((k) => k as CoID<RawCoValue>)
        : [];

    const visibleRows = coIdArray.slice(0, visibleRowsCount);

    return [coIdArray, visibleRows];
  }, [data, visibleRowsCount]);
  const resolvedRows = useResolvedCoValues(visibleRows, node);

  const hasMore = visibleRowsCount < coIdArray.length;

  if (!coIdArray.length) {
    return <div>No data to display</div>;
  }

  if (resolvedRows.length === 0) {
    return <div>Loading...</div>;
  }

  const keys = Array.from(
    new Set(resolvedRows.flatMap((item) => Object.keys(item.snapshot || {}))),
  );

  const loadMore = () => {
    setVisibleRowsCount((prevVisibleRows) => prevVisibleRows + 10);
  };

  return (
    <div>
      <table className="min-w-full border-spacing-0 border-collapse">
        <thead className="sticky top-0 border-b border-gray-200">
          <tr>
            {["", ...keys].map((key) => (
              <th
                key={key}
                className="p-3 bg-gray-50  dark:bg-gray-925 text-left text-xs font-medium text-gray-500 rounded"
              >
                {key}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className=" border-t border-gray-200">
          {resolvedRows.slice(0, visibleRowsCount).map((item, index) => (
            <tr key={index}>
              <td className="p-1">
                <button
                  onClick={() =>
                    onNavigate([
                      {
                        coId: item.value!.id,
                        name: index.toString(),
                      },
                    ])
                  }
                  className="p-4 whitespace-nowrap text-sm text-gray-500 rounded hover:bg-gray-100 hover:text-blue-500"
                >
                  <LinkIcon />
                </button>
              </td>
              {keys.map((key) => (
                <td
                  key={key}
                  className="p-4 whitespace-nowrap text-sm text-gray-500"
                >
                  <ValueRenderer
                    json={(item.snapshot as JsonObject)[key]}
                    onCoIDClick={(coId) => {
                      async function handleClick() {
                        onNavigate([
                          {
                            coId: item.value!.id,
                            name: index.toString(),
                          },
                          {
                            coId: coId,
                            name: key,
                          },
                        ]);
                      }

                      handleClick();
                    }}
                  />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      <div className="py-4 text-gray-500 flex items-center justify-between gap-2">
        <span>
          Showing {Math.min(visibleRowsCount, coIdArray.length)} of{" "}
          {coIdArray.length}
        </span>
        {hasMore && (
          <div className="text-center">
            <button
              onClick={loadMore}
              className="px-4 py-2 bg-blue text-white rounded hover:bg-blue-800"
            >
              Load more
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
