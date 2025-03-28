import React from "react";
import { Button } from "../ui/button.js";
import { PageInfo } from "./types.js";

import { classNames } from "../utils.js";

interface BreadcrumbsProps {
  path: PageInfo[];
  onBreadcrumbClick: (index: number) => void;
}

export const Breadcrumbs: React.FC<BreadcrumbsProps> = ({
  path,
  onBreadcrumbClick,
}) => {
  return (
    <div className={classNames("relative z-20 flex-1 flex items-center")}>
      <Button
        variant="link"
        className={classNames("px-1")}
        onClick={() => onBreadcrumbClick(-1)}
      >
        Home
      </Button>
      {path.map((page, index) => {
        return (
          <React.Fragment key={page.coId}>
            <span
              aria-hidden
              className={classNames(
                "text-stone-400 dark:text-stone-600 px-0.5",
              )}
            >
              /
            </span>
            <Button
              variant="link"
              className={classNames("px-1")}
              onClick={() => onBreadcrumbClick(index)}
            >
              {index === 0 ? page.name || "Root" : page.name}
            </Button>
          </React.Fragment>
        );
      })}
    </div>
  );
};
