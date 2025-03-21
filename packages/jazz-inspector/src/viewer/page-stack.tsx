import { CoID, LocalNode, RawCoValue } from "cojson";
import { Page } from "./page.js"; // Assuming you have a Page component

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
    <div className="relative px-3 overflow-y-auto flex-1  text-stone-700 dark:text-stone-400">
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
    </div>
  );
}
