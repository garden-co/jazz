import { CoID, LocalNode, RawCoValue } from "cojson";
import { JsonObject } from "cojson/src/jsonValue.ts";
import { ResolveIcon } from "./type-icon.tsx";
import { PageInfo, isCoId } from "./types.ts";
import { CoMapPreview, ValueRenderer } from "./value-renderer.tsx";

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
      style={{
        display: "grid",
        gridTemplateColumns: "1fr",
        gap: "1rem",
        padding: "0.5rem",
      }}
    >
      {entries.map(([key, child], childIndex) => (
        <div
          key={childIndex}
          style={{
            padding: "0.75rem",
            borderRadius: "0.5rem",
            overflow: "hidden",
            transition: "background-color 0.2s",
            ...(isCoId(child)
              ? {
                  backgroundColor: "white",
                  border: "1px solid #e5e7eb",
                  cursor: "pointer",
                  boxShadow: "0 1px 2px 0 rgba(0, 0, 0, 0.05)",
                  ":hover": {
                    backgroundColor: "rgba(243, 244, 246, 0.05)",
                  },
                }
              : {
                  backgroundColor: "rgb(249, 250, 251)",
                }),
          }}
          onClick={() =>
            isCoId(child) &&
            onNavigate([{ coId: child as CoID<RawCoValue>, name: key }])
          }
        >
          <h3
            style={{
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {isCoId(child) ? (
              <span
                style={{
                  fontWeight: 500,
                  display: "flex",
                  justifyContent: "space-between",
                }}
              >
                {key}

                <div
                  style={{
                    padding: "0.25rem 0.5rem",
                    fontSize: "0.75rem",
                    backgroundColor: "rgb(243, 244, 246)",
                    borderRadius: "0.25rem",
                  }}
                >
                  <ResolveIcon coId={child as CoID<RawCoValue>} node={node} />
                </div>
              </span>
            ) : (
              <span>{key}</span>
            )}
          </h3>
          <div style={{ marginTop: "0.5rem", fontSize: "0.875rem" }}>
            {isCoId(child) ? (
              <CoMapPreview coId={child as CoID<RawCoValue>} node={node} />
            ) : (
              <ValueRenderer
                json={child}
                onCoIDClick={(coId) => {
                  onNavigate([{ coId, name: key }]);
                }}
                compact
              />
            )}
          </div>
        </div>
      ))}
    </div>
  );
}
