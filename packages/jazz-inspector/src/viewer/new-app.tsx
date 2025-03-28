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

type Position =
  | "bottom right"
  | "bottom left"
  | "top right"
  | "top left"
  | "right"
  | "left";

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

  const positionClasses = {
    "bottom right": "bottom-0 right-0",
    "bottom left": "bottom-0 left-0",
    "top right": "top-0 right-0",
    "top left": "top-0 left-0",
    right: "right-0 top-1/2 -translate-y-1/2",
    left: "left-0 top-1/2 -translate-y-1/2",
  };

  if (!open) {
    // not sure if this will work, probably is better to use inline styles for the button, but please check.

    return (
      <Button
        id="__jazz_inspector"
        variant="secondary"
        onClick={() => setOpen(true)}
        className={classNames(
          `fixed w-10 h-10 bg-white shadow-sm bottom-0 right-0 m-4 p-1.5 ${positionClasses[position]}`,
        )}
      >
        <svg
          className={classNames("w-full h-auto relative -left-px text-blue")}
          xmlns="http://www.w3.org/2000/svg"
          width="119"
          height="115"
          viewBox="0 0 119 115"
          fill="none"
        >
          <path
            fillRule="evenodd"
            clipRule="evenodd"
            d="M118.179 23.8277V0.167999C99.931 7.5527 79.9854 11.6192 59.0897 11.6192C47.1466 11.6192 35.5138 10.2908 24.331 7.7737V30.4076V60.1508C23.2955 59.4385 22.1568 58.8458 20.9405 58.3915C18.1732 57.358 15.128 57.0876 12.1902 57.6145C9.2524 58.1414 6.5539 59.4419 4.4358 61.3516C2.3178 63.2613 0.875401 65.6944 0.291001 68.3433C-0.293399 70.9921 0.00659978 73.7377 1.1528 76.2329C2.2991 78.728 4.2403 80.861 6.7308 82.361C9.2214 83.862 12.1495 84.662 15.1448 84.662C15.6054 84.662 15.8365 84.662 16.0314 84.659C26.5583 84.449 35.042 75.9656 35.2513 65.4386C35.2534 65.3306 35.2544 65.2116 35.2548 65.0486L35.2552 64.7149V64.5521V61.0762V32.1993C43.0533 33.2324 51.0092 33.7656 59.0897 33.7656C59.6696 33.7656 60.2489 33.7629 60.8276 33.7574V89.696C59.792 88.983 58.6533 88.391 57.437 87.936C54.6697 86.903 51.6246 86.632 48.6867 87.159C45.7489 87.686 43.0504 88.987 40.9323 90.896C38.8143 92.806 37.3719 95.239 36.7875 97.888C36.2032 100.537 36.5031 103.283 37.6494 105.778C38.7956 108.273 40.7368 110.405 43.2273 111.906C45.7179 113.406 48.646 114.207 51.6414 114.207C52.1024 114.207 52.3329 114.207 52.5279 114.203C63.0548 113.994 71.5385 105.51 71.7478 94.983C71.7517 94.788 71.7517 94.558 71.7517 94.097V90.621V33.3266C83.962 32.4768 95.837 30.4075 107.255 27.2397V59.9017C106.219 59.1894 105.081 58.5966 103.864 58.1424C101.097 57.1089 98.052 56.8384 95.114 57.3653C92.176 57.8922 89.478 59.1927 87.36 61.1025C85.242 63.0122 83.799 65.4453 83.215 68.0941C82.631 70.743 82.931 73.4886 84.077 75.9837C85.223 78.4789 87.164 80.612 89.655 82.112C92.145 83.612 95.073 84.413 98.069 84.413C98.53 84.413 98.76 84.413 98.955 84.409C109.482 84.2 117.966 75.7164 118.175 65.1895C118.179 64.9945 118.179 64.764 118.179 64.3029V60.8271V23.8277Z"
            fill="currentColor"
          />
        </svg>
        <span className={classNames("sr-only")}>Open Jazz Inspector</span>
      </Button>
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
