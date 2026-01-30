import { CoID, LocalNode, RawAccount } from "cojson";
import { styled } from "goober";
import { useCallback, useState } from "react";
import { PageStack } from "./viewer/page-stack.js";
import { GlobalStyles } from "./ui/global-styles.js";
import { InspectorButton, type Position } from "./viewer/inspector-button.js";
import { useOpenInspector } from "./viewer/use-open-inspector.js";
import { NodeProvider } from "./contexts/node.js";
import { InMemoryRouterProvider } from "./router/in-memory-router.js";
import { Header } from "./viewer/header.js";
import { PerformancePage } from "./pages/performance/index.js";
import { HomePage } from "./pages/home.js";

export type InspectorTab = "inspector" | "performance";

const STORAGE_KEY = "jazz-inspector-tab";

function getStoredTab(): InspectorTab {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === "inspector" || stored === "performance") {
      return stored;
    }
  } catch {
    // localStorage not available
  }
  return "inspector";
}

export function InspectorInApp({
  position = "right",
  localNode,
  accountId,
  showDeleteLocalData = false,
}: {
  position?: Position;
  localNode?: LocalNode;
  accountId?: CoID<RawAccount>;
  showDeleteLocalData?: boolean;
}) {
  const [open, setOpen] = useOpenInspector();
  const [activeTab, setActiveTabState] = useState<InspectorTab>(getStoredTab);

  const setActiveTab = useCallback((tab: InspectorTab) => {
    setActiveTabState(tab);
    try {
      localStorage.setItem(STORAGE_KEY, tab);
    } catch {
      // localStorage not available
    }
  }, []);

  if (!open) {
    return (
      <InspectorButton position={position} onClick={() => setOpen(true)} />
    );
  }

  return (
    <NodeProvider localNode={localNode ?? null} accountID={accountId ?? null}>
      <InMemoryRouterProvider>
        <InspectorContainer as={GlobalStyles} style={{ zIndex: 999 }}>
          <Header
            showClose={true}
            onClose={() => setOpen(false)}
            activeTab={activeTab}
            onTabChange={setActiveTab}
          />
          {/* Both components stay mounted, visibility controlled by CSS */}
          <PageStack
            style={{ display: activeTab === "inspector" ? "flex" : "none" }}
            homePage={<HomePage showDeleteLocalData={showDeleteLocalData} />}
          />
          <PerformancePage
            style={{ display: activeTab === "performance" ? "flex" : "none" }}
            onNavigate={() => setActiveTab("inspector")}
          />
        </InspectorContainer>
      </InMemoryRouterProvider>
    </NodeProvider>
  );
}

const InspectorContainer = styled("div")`
  position: fixed;
  height: 50vh;
  max-height: 800px;
  display: flex;
  flex-direction: column;
  bottom: 0;
  left: 0;
  width: 100%;
  background-color: white;
  border-top: 1px solid var(--j-border-color);
  color: var(--j-text-color);

  @media (prefers-color-scheme: dark) {
    background-color: var(--j-background);
  }
`;
