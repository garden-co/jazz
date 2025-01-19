import { CoID, LocalNode, RawCoValue } from "cojson";
import { Page } from "./page.tsx"; // Assuming you have a Page component

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
    <div
      style={{
        position: "relative",
        marginTop: "1rem",
        height: "40vh",
        overflowY: "auto",
      }}
    >
      {children && (
        <div
          style={{
            position: "absolute",
            top: 0,
            right: 0,
            bottom: 0,
            left: 0,
            paddingBottom: "5rem",
          }}
        >
          {children}
        </div>
      )}
      {node && page && (
        <Page
          coId={page.coId}
          node={node}
          name={page.name || page.coId}
          onHeaderClick={goBack}
          onNavigate={addPages}
          isTopLevel={index === path.length - 1}
          style={{
            transform: `translateZ(${(index - path.length + 1) * 200}px) scale(${
              1 - (path.length - index - 1) * 0.05
            }) translateY(${-(index - path.length + 1) * -4}%)`,
            opacity: 1 - (path.length - index - 1) * 0.05,
            zIndex: index,
            transitionProperty: "transform, opacity",
            transitionDuration: "0.3s",
            transitionTimingFunction: "ease-out",
          }}
        />
      )}
    </div>
  );
}
