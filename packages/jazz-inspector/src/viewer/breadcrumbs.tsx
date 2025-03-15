import React from "react";
import { PageInfo } from "./types.js";

interface BreadcrumbsProps {
  path: PageInfo[];
  onBreadcrumbClick: (index: number) => void;
}

export const Breadcrumbs: React.FC<BreadcrumbsProps> = ({
  path,
  onBreadcrumbClick,
}) => {
  return (
    <div className="relative z-20 bg-blue-400/10 backdrop-blur-sm rounded-lg inline-flex px-2 py-1 whitespace-pre transition-all items-center gap-1 min-h-[2.5rem]">
      <button
        onClick={() => onBreadcrumbClick(-1)}
        className="flex items-center justify-center p-1 rounded-sm transition-colors"
        aria-label="Go to home"
      >
        Start
      </button>
      {path.map((page, index) => {
        return (
          <span
            key={index}
            className={`inline-block ${index === 0 ? "pl-1" : "pl-0"} ${
              index === path.length - 1 ? "pr-1" : "pr-0"
            }`}
          >
            {index === 0 ? null : (
              <span className="text-blue-600/30">{" / "}</span>
            )}
            <button
              onClick={() => onBreadcrumbClick(index)}
              className="text-blue hover:underline dark:text-blue-400"
            >
              {index === 0 ? page.name || "Root" : page.name}
            </button>
          </span>
        );
      })}
    </div>
  );
};
