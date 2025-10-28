import { CoID, LocalNode, RawCoValue } from "cojson";
import { styled } from "goober";
import { Page } from "./page.js";

// Define the structure of a page in the path
interface PageInfo {
  coId: CoID<RawCoValue>;
  name?: string;
}

// Props for the PageStack component
interface PageStackProps {
  path: PageInfo[];
  node?: LocalNode | null;
  goBack: () => void;
  addPages: (pages: PageInfo[]) => void;
  children?: React.ReactNode;
}

const PageStackContainer = styled("div")`
  position: relative;
  padding: 0 0.75rem;
  overflow-y: auto;
  flex: 1;
  color: var(--j-text-color);
  font-size: 16px;
`;

export function PageStack({
  path,
  node,
  goBack,
  addPages,
  children,
}: PageStackProps) {
  const page = path[path.length - 1];
  const index = path.length - 1;

  return (
    <>
      <PageStackContainer>
        {children}
        {node && page && (
          <Page
            coId={page.coId}
            node={node}
            name={page.name || page.coId}
            onHeaderClick={goBack}
            onNavigate={addPages}
            isTopLevel={index === path.length - 1}
          />
        )}
      </PageStackContainer>
    </>
  );
}
