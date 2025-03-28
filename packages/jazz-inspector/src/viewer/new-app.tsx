import { CoID, RawCoValue } from "cojson";
import { useAccount } from "jazz-react-core";
import React, { useState } from "react";
import { Button } from "../ui/button.js";
import { Input } from "../ui/input.js";
import { Breadcrumbs } from "./breadcrumbs.js";
import { PageStack } from "./page-stack.js";
import { usePagePath } from "./use-page-path.js";

import { Heading } from "../ui/heading.js";
import { classNames } from "../utils.js";
import { InspectorButton, type Position } from "./inpsector-button.js";

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
    <div
      className={classNames(
        "fixed h-[calc(100%-12rem)] flex flex-col bottom-0 left-0 w-full bg-white border-t border-gray-200 dark:border-stone-900 dark:bg-stone-925",
      )}
      id="__jazz_inspector"
    >
      <div className={classNames("flex items-center gap-4 px-3 my-3")}>
        <Breadcrumbs path={path} onBreadcrumbClick={goToIndex} />
        <form onSubmit={handleCoValueIdSubmit} className={classNames("w-96")}>
          {path.length !== 0 && (
            <Input
              label="CoValue ID"
              className={classNames("font-mono")}
              hideLabel
              placeholder="co_z1234567890abcdef123456789"
              value={coValueId}
              onChange={(e) => setCoValueId(e.target.value as CoID<RawCoValue>)}
            />
          )}
        </form>
        <Button variant="plain" type="button" onClick={() => setOpen(false)}>
          Close
        </Button>
      </div>

      <PageStack
        path={path}
        node={localNode}
        goBack={goBack}
        addPages={addPages}
      >
        {path.length <= 0 && (
          <form
            onSubmit={handleCoValueIdSubmit}
            aria-hidden={path.length !== 0}
            className={classNames(
              "flex flex-col relative -top-6 justify-center gap-2 h-full w-full max-w-sm mx-auto",
            )}
          >
            <Heading>Jazz CoValue Inspector</Heading>
            <Input
              label="CoValue ID"
              className={classNames("min-w-[21rem] font-mono")}
              hideLabel
              placeholder="co_z1234567890abcdef123456789"
              value={coValueId}
              onChange={(e) => setCoValueId(e.target.value as CoID<RawCoValue>)}
            />
            <Button type="submit" variant="primary">
              Inspect CoValue
            </Button>

            <p className={classNames("text-center")}>or</p>

            <Button
              variant="secondary"
              onClick={() => {
                setCoValueId(me._raw.id);
                setPage(me._raw.id);
              }}
            >
              Inspect my account
            </Button>
          </form>
        )}
      </PageStack>
    </div>
  );
}
