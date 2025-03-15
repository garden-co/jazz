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
    <div className="relative mt-4 h-[40vh] overflow-y-auto">
      {children && <div className="absolute inset-0 pb-20">{children}</div>}
      {node && page && (
        <Page
          coId={page.coId}
          node={node}
          name={page.name || page.coId}
          onHeaderClick={goBack}
          onNavigate={addPages}
          isTopLevel={index === path.length - 1}
          className="transition-transform transition-opacity duration-300 ease-out"
          style={{
            transform: `translateZ(${(index - path.length + 1) * 200}px) scale(${
              1 - (path.length - index - 1) * 0.05
            }) translateY(${-(index - path.length + 1) * -4}%)`,
            opacity: 1 - (path.length - index - 1) * 0.05,
            zIndex: index,
          }}
        />
      )}
    </div>
  );
}
