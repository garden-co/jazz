import { CoID, RawCoValue } from "cojson";
import { useAccount } from "jazz-react-core";
import React, { useState } from "react";
import { Breadcrumbs } from "./breadcrumbs.js";
import { PageStack } from "./page-stack.js";
import { usePagePath } from "./use-page-path.js";

export function JazzInspector() {
  const [open, setOpen] = useState(false);
  const [coValueId, setCoValueId] = useState<CoID<RawCoValue> | "">("");
  const { path, addPages, goToIndex, goBack, setPage } = usePagePath();

  const { me } = useAccount();
  const localNode = me._raw.core.node;

  const handleCoValueIdSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (coValueId) {
      setPage(coValueId);
    }
  };

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        style={{
          position: "fixed",
          bottom: 0,
          right: 0,
          margin: "1rem",
          backgroundColor: "white",
          border: "1px solid #e5e7eb",
          borderRadius: "0.5rem",
          padding: "6px",
        }}
      >
        Jazz Inspector
      </button>
    );
  }

  return (
    <div
      style={{
        position: "fixed",
        bottom: 0,
        left: 0,
        width: "100%",
        backgroundColor: "white",
        borderTop: "1px solid #e5e7eb",
        padding: "1rem",
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: "1rem",
        }}
      >
        <Breadcrumbs path={path} onBreadcrumbClick={goToIndex} />
        <button onClick={() => setOpen(false)}>Close</button>
      </div>

      <PageStack
        path={path}
        node={localNode}
        goBack={goBack}
        addPages={addPages}
      >
        <form
          onSubmit={handleCoValueIdSubmit}
          aria-hidden={path.length !== 0}
          style={{
            display: "flex",
            flexDirection: "column",
            justifyContent: "center",
            alignItems: "center",
            gap: "0.5rem",
            height: "100%",
            width: "100%",
            marginBottom: "5rem",
            transition: "all 150ms",
            opacity: path.length > 0 ? 0 : 1,
            transform:
              path.length > 0 ? "translateY(-0.5rem) scale(0.95)" : "none",
          }}
        >
          <fieldset
            style={{
              display: "flex",
              flexDirection: "column",
              gap: "0.5rem",
              fontSize: "0.875rem",
            }}
          >
            <h2
              style={{
                fontSize: "1.875rem",
                fontWeight: 500,
                color: "#030712",
                textAlign: "center",
                marginBottom: "1rem",
              }}
            >
              Jazz CoValue Inspector
            </h2>
            <input
              style={{
                border: "1px solid #e5e7eb",
                padding: "1rem",
                borderRadius: "0.5rem",
                minWidth: "21rem",
                fontFamily: "monospace",
              }}
              placeholder="co_z1234567890abcdef123456789"
              value={coValueId}
              onChange={(e) => setCoValueId(e.target.value as CoID<RawCoValue>)}
            />
            <button
              type="submit"
              style={{
                backgroundColor: "rgb(99 102 241)",
                color: "white",
                padding: "0.5rem 1rem",
                borderRadius: "0.375rem",
              }}
              onMouseOver={(e) =>
                (e.currentTarget.style.backgroundColor =
                  "rgba(99 102 241, 0.8)")
              }
              onMouseOut={(e) =>
                (e.currentTarget.style.backgroundColor = "rgb(99 102 241)")
              }
            >
              Inspect
            </button>
            <hr />
            <button
              type="button"
              style={{
                border: "1px solid #e5e7eb",
                display: "inline-block",
                padding: "0.375rem 0.5rem",
                color: "black",
                borderRadius: "0.375rem",
              }}
              onClick={() => {
                setCoValueId(me._raw.id);
                setPage(me._raw.id);
              }}
            >
              Inspect My Account
            </button>
          </fieldset>
        </form>
      </PageStack>
    </div>
  );
}
