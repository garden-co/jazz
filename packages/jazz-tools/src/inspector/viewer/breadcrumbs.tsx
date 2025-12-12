import { styled } from "goober";
import React from "react";
import { Button } from "../ui/button.js";
import { useRouter } from "../router/context.js";

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

export const Breadcrumbs: React.FC<{}> = () => {
  const { path, goToIndex } = useRouter();

  return (
    <BreadcrumbsContainer>
      <Button
        variant="link"
        style={{ padding: "0 0.25rem" }}
        onClick={() => goToIndex(-1)}
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
              onClick={() => goToIndex(index)}
            >
              {index === 0 ? page.name || "Root" : page.name}
            </Button>
          </React.Fragment>
        );
      })}
    </BreadcrumbsContainer>
  );
};
