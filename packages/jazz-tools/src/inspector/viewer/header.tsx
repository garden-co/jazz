import { CoID, RawCoValue } from "cojson";
import { styled } from "goober";
import React, { type PropsWithChildren, useState } from "react";
import { Button } from "../ui/button.js";
import { Input } from "../ui/input.js";
import { Breadcrumbs } from "./breadcrumbs.js";
import { useRouter } from "../router/context.js";
import type { InspectorTab } from "../in-app.js";

export function Header({
  showClose = false,
  onClose,
  activeTab,
  onTabChange,
  children,
}: PropsWithChildren<{
  showClose?: boolean;
  onClose?: () => void;
  activeTab?: InspectorTab;
  onTabChange?: (tab: InspectorTab) => void;
}>) {
  const [coValueId, setCoValueId] = useState<CoID<RawCoValue> | "">("");
  const { path, setPage } = useRouter();

  const handleCoValueIdSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (coValueId) {
      setPage(coValueId);
    }
    setCoValueId("");
  };

  return (
    <HeaderContainer>
      {activeTab && onTabChange && (
        <TabBar>
          <Tab
            active={activeTab === "inspector"}
            onClick={() => onTabChange("inspector")}
          >
            Inspector
          </Tab>
          <Tab
            active={activeTab === "performance"}
            onClick={() => onTabChange("performance")}
          >
            Performance
          </Tab>
        </TabBar>
      )}
      {(activeTab === "inspector" || !activeTab) && (
        <>
          <Breadcrumbs />
          {path.length !== 0 && (
            <Form onSubmit={handleCoValueIdSubmit}>
              <Input
                label="CoValue ID"
                style={{ fontFamily: "monospace" }}
                hideLabel
                placeholder="co_z1234567890abcdef123456789"
                value={coValueId}
                onChange={(e) =>
                  setCoValueId(e.target.value as CoID<RawCoValue>)
                }
              />
            </Form>
          )}
        </>
      )}
      {children}
      <Spacer />
      {showClose && (
        <Button variant="plain" type="button" onClick={onClose}>
          <svg
            width="14"
            height="14"
            viewBox="0 0 14 14"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              d="M1 1L13 13M1 13L13 1"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
            />
          </svg>
        </Button>
      )}
    </HeaderContainer>
  );
}

const HeaderContainer = styled("div")`
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 0 0.75rem;
  margin: 0.75rem 0;
`;

const Form = styled("form")`
  width: 24rem;
`;

const TabBar = styled("div")`
  display: flex;
  gap: 0.25rem;
  background-color: var(--j-foreground);
  border-radius: var(--j-radius-lg);
  padding: 0.25rem;
`;

const Tab = styled("button")<{ active?: boolean }>`
  padding: 0.375rem 0.75rem;
  border: none;
  border-radius: var(--j-radius-md);
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.15s ease;

  ${(props) =>
    props.active
      ? `
    background-color: white;
    color: var(--j-text-color-strong);
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);

    @media (prefers-color-scheme: dark) {
      background-color: var(--j-neutral-800);
    }
  `
      : `
    background-color: transparent;
    color: var(--j-neutral-500);

    &:hover {
      color: var(--j-text-color-strong);
    }
  `}
`;

const Spacer = styled("div")`
  flex: 1;
`;
