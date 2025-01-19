import React from "react";
import { PageInfo } from "./types.ts";

interface BreadcrumbsProps {
  path: PageInfo[];
  onBreadcrumbClick: (index: number) => void;
}

export const Breadcrumbs: React.FC<BreadcrumbsProps> = ({
  path,
  onBreadcrumbClick,
}) => {
  return (
    <div
      style={{
        position: "relative",
        zIndex: 20,
        backgroundColor: "rgba(129, 140, 248, 0.1)", // indigo-400/10 equivalent
        backdropFilter: "blur(4px)",
        borderRadius: "0.5rem",
        display: "inline-flex",
        paddingLeft: "0.5rem",
        paddingRight: "0.5rem",
        paddingTop: "0.25rem",
        paddingBottom: "0.25rem",
        whiteSpace: "pre",
        transition: "all",
        alignItems: "center",
        gap: "0.25rem",
        minHeight: "2.5rem",
      }}
    >
      <button
        onClick={() => onBreadcrumbClick(-1)}
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: "0.25rem",
          borderRadius: "0.125rem",
          transition: "colors",
        }}
        onMouseOver={(e) =>
          (e.currentTarget.style.backgroundColor = "rgba(99, 102, 241, 0.1)")
        }
        onMouseOut={(e) =>
          (e.currentTarget.style.backgroundColor = "transparent")
        }
        aria-label="Go to home"
      >
        Start
      </button>
      {path.map((page, index) => {
        return (
          <span
            key={index}
            style={{
              display: "inline-block",
              paddingLeft: index === 0 ? "0.25rem" : "0",
              paddingRight: index === path.length - 1 ? "0.25rem" : "0",
            }}
          >
            {index === 0 ? null : (
              <span style={{ color: "rgba(99, 102, 241, 0.3)" }}>{" / "}</span>
            )}
            <button
              onClick={() => onBreadcrumbClick(index)}
              style={{
                color: "rgb(67, 56, 202)",
              }}
              onMouseOver={(e) =>
                (e.currentTarget.style.textDecoration = "underline")
              }
              onMouseOut={(e) =>
                (e.currentTarget.style.textDecoration = "none")
              }
            >
              {index === 0 ? page.name || "Root" : page.name}
            </button>
          </span>
        );
      })}
    </div>
  );
};
