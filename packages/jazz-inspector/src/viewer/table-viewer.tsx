import { CoID, LocalNode, RawCoValue } from "cojson";
import type { JsonObject } from "cojson/src/jsonValue.ts";
import { useMemo, useState } from "react";
import { LinkIcon } from "../link-icon.tsx";
import { PageInfo } from "./types.ts";
import { useResolvedCoValues } from "./use-resolve-covalue.ts";
import { ValueRenderer } from "./value-renderer.tsx";

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
      <table
        style={{
          minWidth: "100%",
          borderSpacing: 0,
          borderCollapse: "collapse",
        }}
      >
        <thead
          style={{
            position: "sticky",
            top: 0,
            borderBottom: "1px solid #e5e7eb",
          }}
        >
          <tr>
            {["", ...keys].map((key) => (
              <th
                key={key}
                style={{
                  padding: "0.75rem 1rem",
                  backgroundColor: "#f9fafb",
                  textAlign: "left",
                  fontSize: "0.75rem",
                  fontWeight: 500,
                  color: "#6b7280",
                  borderRadius: "0.25rem",
                }}
              >
                {key}
              </th>
            ))}
          </tr>
        </thead>
        <tbody
          style={{ backgroundColor: "white", borderTop: "1px solid #e5e7eb" }}
        >
          {resolvedRows.slice(0, visibleRowsCount).map((item, index) => (
            <tr key={index}>
              <td style={{ padding: "0.25rem 0.25rem" }}>
                <button
                  onClick={() =>
                    onNavigate([
                      {
                        coId: item.value!.id,
                        name: index.toString(),
                      },
                    ])
                  }
                  style={{
                    padding: "1rem",
                    whiteSpace: "nowrap",
                    fontSize: "0.875rem",
                    color: "#6b7280",
                    borderRadius: "0.25rem",
                  }}
                  onMouseOver={(e) => {
                    e.currentTarget.style.backgroundColor = "#f3f4f6";
                    e.currentTarget.style.color = "#3b82f6";
                  }}
                  onMouseOut={(e) => {
                    e.currentTarget.style.backgroundColor = "transparent";
                    e.currentTarget.style.color = "#6b7280";
                  }}
                >
                  <LinkIcon />
                </button>
              </td>
              {keys.map((key) => (
                <td
                  key={key}
                  style={{
                    padding: "1rem",
                    whiteSpace: "nowrap",
                    fontSize: "0.875rem",
                    color: "#6b7280",
                  }}
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
      <div
        style={{
          padding: "1rem 0",
          color: "#6b7280",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: "0.5rem",
        }}
      >
        <span>
          Showing {Math.min(visibleRowsCount, coIdArray.length)} of{" "}
          {coIdArray.length}
        </span>
        {hasMore && (
          <div style={{ textAlign: "center" }}>
            <button
              onClick={loadMore}
              style={{
                padding: "0.5rem 1rem",
                backgroundColor: "#3b82f6",
                color: "white",
                borderRadius: "0.25rem",
              }}
              onMouseOver={(e) => {
                e.currentTarget.style.backgroundColor = "#2563eb";
              }}
              onMouseOut={(e) => {
                e.currentTarget.style.backgroundColor = "#3b82f6";
              }}
            >
              Load More
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
