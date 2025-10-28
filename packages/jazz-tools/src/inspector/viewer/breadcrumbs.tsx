import { styled } from "goober";
import React from "react";
import { Button } from "../ui/button.js";
import { PageInfo } from "./types.js";

const BreadcrumbsContainer = styled("div")`
  position: relative;
  z-index: 20;
  flex: 1;
  display: flex;
  align-items: center;
`;

const Separator = styled("span")`
  padding: 0 0.125rem;
`;

interface BreadcrumbsProps {
  path: PageInfo[];
  onBreadcrumbClick: (index: number) => void;
}

export const Breadcrumbs: React.FC<BreadcrumbsProps> = ({
  path,
  onBreadcrumbClick,
}) => {
  return (
    <BreadcrumbsContainer>
      <Button
        variant="link"
        style={{ padding: "0 0.25rem" }}
        onClick={() => onBreadcrumbClick(-1)}
      >
        Home
      </Button>
      {path.map((page, index) => {
        return (
          <React.Fragment key={page.coId}>
            <Separator aria-hidden>/</Separator>
            <Button
              variant="link"
              style={{ padding: "0 0.25rem" }}
              onClick={() => onBreadcrumbClick(index)}
            >
              {index === 0 ? page.name || "Root" : page.name}
            </Button>
          </React.Fragment>
        );
      })}
    </BreadcrumbsContainer>
  );
};
