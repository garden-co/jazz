import { CoID, LocalNode, RawAccount } from "cojson";
import { styled } from "goober";
import { useEffect, useRef } from "react";
import { PageStack } from "./viewer/page-stack.js";
import { GlobalStyles } from "./ui/global-styles.js";
import { InspectorButton, type Position } from "./viewer/inspector-button.js";
import { useOpenInspector } from "./viewer/use-open-inspector.js";
import { NodeProvider } from "./contexts/node.js";
import { InMemoryRouterProvider } from "./router/in-memory-router.js";
import { Header } from "./viewer/header.js";

let instanceCount = 0;

export function InspectorInApp({
  position = "right",
  localNode,
  accountId,
}: {
  position?: Position;
  localNode?: LocalNode;
  accountId?: CoID<RawAccount>;
}) {
  const [open, setOpen] = useOpenInspector();
  const hasWarnedRef = useRef(false);

  useEffect(() => {
    instanceCount++;

    if (instanceCount > 1 && !hasWarnedRef.current) {
      console.error(
        `[InspectorInApp] Multiple instances detected (${instanceCount}). Only one InspectorInApp should be rendered at a time.`,
      );
      hasWarnedRef.current = true;
    }

    return () => {
      instanceCount--;
    };
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
            showDeleteLocalData={true}
            showClose={true}
            onClose={() => setOpen(false)}
            showPerformance={true}
          />

          <PageStack />
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
