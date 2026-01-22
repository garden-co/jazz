import { CoID, LocalNode, RawCoValue } from "cojson";
import { styled } from "goober";
import type { CSSProperties } from "react";
import { Page } from "./page.js";
import { ErrorBoundary } from "../ui/error-boundary.js";
import { useRouter } from "../router/context.js";
import { useNode } from "../contexts/node.js";
import { HomePage } from "../pages/home.js";

const PageStackContainer = styled("article")`
  position: relative;
  padding: 0 0.75rem;
  overflow-y: auto;
  flex: 1;
  color: var(--j-text-color);
  font-size: 16px;
`;

type PageStackProps = {
  homePage?: React.ReactNode;
  style?: CSSProperties;
};

export function PageStack({ homePage, style }: PageStackProps) {
  const { path, addPages, goBack } = useRouter();
  const { localNode } = useNode();

  const page = path[path.length - 1];
  const index = path.length - 1;

  if (path.length <= 0) {
    return (
      <PageStackContainer style={style}>
        {homePage ?? <HomePage />}
      </PageStackContainer>
    );
  }

  return (
    <PageStackContainer style={style}>
      {localNode && page && (
        <ErrorBoundary title="An error occurred while rendering this CoValue">
          <Page
            coId={page.coId}
            node={localNode}
            name={page.name || page.coId}
            onHeaderClick={goBack}
            onNavigate={addPages}
            isTopLevel={index === path.length - 1}
          />
        </ErrorBoundary>
      )}
    </PageStackContainer>
  );
}
