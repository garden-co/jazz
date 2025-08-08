import { CoID, LocalNode, RawAccount, RawCoValue } from "cojson";
import { styled } from "goober";
import { Account } from "jazz-tools";
import { useJazzContext } from "jazz-tools/react-core";
import React, { useState } from "react";
import { Button } from "../ui/button.js";
import { GlobalStyles } from "../ui/global-styles.js";
import { Heading } from "../ui/heading.js";
import { Input } from "../ui/input.js";
import { Breadcrumbs } from "./breadcrumbs.js";
import { InspectorButton, type Position } from "./inpsector-button.js";
import { PageStack } from "./page-stack.js";
import { useOpenInspector } from "./use-open-inspector.js";
import { usePagePath } from "./use-page-path.js";

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

const InitialForm = styled("form")`
  display: flex;
  flex-direction: column;
  position: relative;
  top: -1.5rem;
  justify-content: center;
  gap: 0.5rem;
  height: 100%;
  width: 100%;
  max-width: 24rem;
  margin: 0 auto;
`;

const OrText = styled("p")`
  text-align: center;
`;

export function JazzInspector({ position = "right" }: { position?: Position }) {
  const context = useJazzContext<Account>();
  const localNode = context.node;
  const me = "me" in context ? context.me : undefined;

  if (process.env.NODE_ENV !== "development") return null;

  return (
    <JazzInspectorInternal
      position={position}
      localNode={localNode}
      accountId={me?._raw.id}
    />
  );
}

export function JazzInspectorInternal({
  position = "right",
  localNode,
  accountId,
}: {
  position?: Position;
  localNode?: LocalNode;
  accountId?: CoID<RawAccount>;
}) {
  const [open, setOpen] = useOpenInspector();
  const [coValueId, setCoValueId] = useState<CoID<RawCoValue> | "">("");
  const { path, addPages, goToIndex, goBack, setPage } = usePagePath();

  const handleCoValueIdSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (coValueId) {
      setPage(coValueId);
    }
    setCoValueId("");
  };

  if (!open) {
    return (
      <InspectorButton position={position} onClick={() => setOpen(true)} />
    );
  }

  return (
    <InspectorContainer as={GlobalStyles} style={{ zIndex: 999 }}>
      <HeaderContainer>
        <Breadcrumbs path={path} onBreadcrumbClick={goToIndex} />
        <Form onSubmit={handleCoValueIdSubmit}>
          {path.length !== 0 && (
            <Input
              label="CoValue ID"
              style={{ fontFamily: "monospace" }}
              hideLabel
              placeholder="co_z1234567890abcdef123456789"
              value={coValueId}
              onChange={(e) => setCoValueId(e.target.value as CoID<RawCoValue>)}
            />
          )}
        </Form>
        <Button variant="plain" type="button" onClick={() => setOpen(false)}>
          Close
        </Button>
      </HeaderContainer>

      <PageStack
        path={path}
        node={localNode}
        goBack={goBack}
        addPages={addPages}
      >
        {path.length <= 0 && (
          <InitialForm
            onSubmit={handleCoValueIdSubmit}
            aria-hidden={path.length !== 0}
          >
            <Heading>Jazz CoValue Inspector</Heading>
            <Input
              label="CoValue ID"
              style={{ minWidth: "21rem", fontFamily: "monospace" }}
              hideLabel
              placeholder="co_z1234567890abcdef123456789"
              value={coValueId}
              onChange={(e) => setCoValueId(e.target.value as CoID<RawCoValue>)}
            />
            <Button type="submit" variant="primary">
              Inspect CoValue
            </Button>

            {accountId && (
              <>
                <OrText>or</OrText>

                <Button
                  variant="secondary"
                  onClick={() => {
                    setCoValueId(accountId);
                    setPage(accountId);
                  }}
                >
                  Inspect my account
                </Button>
              </>
            )}
          </InitialForm>
        )}
      </PageStack>
    </InspectorContainer>
  );
}
