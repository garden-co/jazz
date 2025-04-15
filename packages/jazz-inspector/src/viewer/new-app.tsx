import { CoID, RawCoValue } from "cojson";
import { styled } from "goober";
import { useAccount } from "jazz-react-core";
import React, { useState } from "react";
import { Button } from "../ui/button.js";
import { Input } from "../ui/input.js";
import { Breadcrumbs } from "./breadcrumbs.js";
import { PageStack } from "./page-stack.js";
import { usePagePath } from "./use-page-path.js";

import { GlobalStyles } from "../ui/global-styles.js";
import { Heading } from "../ui/heading.js";
import { InspectorButton, type Position } from "./inpsector-button.js";

const InspectorContainer = styled("div")`
  position: fixed;
  height: calc(100% - 12rem);
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
  const [open, setOpen] = useState(false);
  const [coValueId, setCoValueId] = useState<CoID<RawCoValue> | "">("");
  const { path, addPages, goToIndex, goBack, setPage } = usePagePath();

  const { me } = useAccount();
  const localNode = me._raw.core.node;

  if (process.env.NODE_ENV !== "development") return;

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

            <OrText>or</OrText>

            <Button
              variant="secondary"
              onClick={() => {
                setCoValueId(me._raw.id);
                setPage(me._raw.id);
              }}
            >
              Inspect my account
            </Button>
          </InitialForm>
        )}
      </PageStack>
    </InspectorContainer>
  );
}
