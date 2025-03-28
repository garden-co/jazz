import { CoID, JsonValue, LocalNode, RawCoValue } from "cojson";
import React, { useEffect, useState } from "react";
import { Button } from "../ui/button.js";
import { Icon } from "../ui/icon.js";
import { classNames } from "../utils.js";
import {
  isBrowserImage,
  resolveCoValue,
  useResolvedCoValue,
} from "./use-resolve-covalue.js";

// Is there a chance we can pass the actual CoValue here?
export function ValueRenderer({
  json,
  onCoIDClick,
}: {
  json: JsonValue | undefined;
  onCoIDClick?: (childNode: CoID<RawCoValue>) => void;
}) {
  const [isExpanded, setIsExpanded] = useState(false);

  if (typeof json === "undefined" || json === undefined) {
    return <span className={classNames("text-gray-400")}>undefined</span>;
  }

  if (json === null) {
    return <span className={classNames("text-gray-400")}>null</span>;
  }

  if (typeof json === "string" && json.startsWith("co_")) {
    const linkClasses = onCoIDClick
      ? "text-blue cursor-pointer inline-flex gap-1 items-center dark:text-blue-400"
      : "inline-flex gap-1 items-center";

    const content = (
      <>
        {json}
        {onCoIDClick && <Icon name="link" />}
      </>
    );

    if (onCoIDClick) {
      return (
        <Button
          className={classNames(linkClasses)}
          onClick={() => {
            onCoIDClick?.(json as CoID<RawCoValue>);
          }}
          variant="plain"
        >
          {content}
        </Button>
      );
    }

    return <span className={classNames(linkClasses)}>{content}</span>;
  }

  if (typeof json === "string") {
    return (
      <span
        className={classNames("text-green-700 font-mono dark:text-green-400")}
      >
        {json}
      </span>
    );
  }

  if (typeof json === "number") {
    return (
      <span className={classNames("text-purple-700 dark:text-purple-400")}>
        {json}
      </span>
    );
  }

  if (typeof json === "boolean") {
    return (
      <span
        className={classNames(
          json
            ? "text-green-700 bg-green-700/5"
            : "text-amber-700 bg-amber-500/5",
          "font-mono",
          "inline-block px-1 py-0.5 rounded",
        )}
      >
        {json.toString()}
      </span>
    );
  }

  if (typeof json === "object") {
    return (
      <span
        title={JSON.stringify(json, null, 2)}
        className={classNames("inline-block max-w-64")}
      >
        <span className={classNames("text-gray-600")}>
          {Array.isArray(json) ? <>Array ({json.length})</> : <>Object</>}
        </span>
        <pre className={classNames("mt-1.5 text-sm whitespace-pre-wrap")}>
          {isExpanded
            ? JSON.stringify(json, null, 2)
            : JSON.stringify(json, null, 2).split("\n").slice(0, 3).join("\n") +
              (Object.keys(json).length > 2 ? "\n..." : "")}
        </pre>
        <Button
          variant="plain"
          onClick={() => setIsExpanded(!isExpanded)}
          className={classNames(
            "mt-1.5 text-sm text-gray-600 hover:text-gray-700",
          )}
        >
          {isExpanded ? "Show less" : "Show more"}
        </Button>
      </span>
    );
  }

  return <span>{String(json)}</span>;
}

export const CoMapPreview = ({
  coId,
  node,
  limit = 6,
}: {
  coId: CoID<RawCoValue>;
  node: LocalNode;
  limit?: number;
}) => {
  const { value, snapshot, type, extendedType } = useResolvedCoValue(
    coId,
    node,
  );

  if (!snapshot) {
    return (
      <div
        className={classNames(
          "rounded bg-gray-100 animate-pulse whitespace-pre w-24",
        )}
      >
        {" "}
      </div>
    );
  }

  if (snapshot === "unavailable" && !value) {
    return <div className={classNames("text-gray-500")}>Unavailable</div>;
  }

  if (extendedType === "image" && isBrowserImage(snapshot)) {
    return (
      <div>
        <img
          src={snapshot.placeholderDataURL}
          className={classNames(
            "size-8 border-2 border-white drop-shadow-md my-2",
          )}
        />
        <span className={classNames("text-gray-500 text-sm")}>
          {snapshot.originalSize[0]} x {snapshot.originalSize[1]}
        </span>

        {/* <CoMapPreview coId={value[]} node={node} /> */}
        {/* <ProgressiveImg image={value}>
                    {({ src }) => <img src={src} className={clsx("w-full")} />}
                </ProgressiveImg> */}
      </div>
    );
  }

  if (extendedType === "record") {
    return (
      <div>
        Record{" "}
        <span className={classNames("text-gray-500")}>
          ({Object.keys(snapshot).length})
        </span>
      </div>
    );
  }

  if (type === "colist") {
    return (
      <div>
        List{" "}
        <span className={classNames("text-gray-500")}>
          ({(snapshot as unknown as []).length})
        </span>
      </div>
    );
  }

  return (
    <div className={classNames("text-sm flex flex-col gap-2 items-start")}>
      <div className={classNames("grid grid-cols-[auto_1fr] gap-2")}>
        {Object.entries(snapshot)
          .slice(0, limit)
          .map(([key, value]) => (
            <React.Fragment key={key}>
              <span className={classNames("font-medium")}>{key}: </span>
              <span>
                <ValueRenderer json={value} />
              </span>
            </React.Fragment>
          ))}
      </div>
      {Object.entries(snapshot).length > limit && (
        <div className={classNames("text-left text-sm text-gray-500 mt-2")}>
          {Object.entries(snapshot).length - limit} more
        </div>
      )}
    </div>
  );
};

export function AccountOrGroupPreview({
  coId,
  node,
  showId = false,
  onClick,
}: {
  coId: CoID<RawCoValue>;
  node: LocalNode;
  showId?: boolean;
  onClick?: (name?: string) => void;
}) {
  const { snapshot, extendedType } = useResolvedCoValue(coId, node);
  const [name, setName] = useState<string | null>(null);

  useEffect(() => {
    if (extendedType === "account") {
      resolveCoValue(
        (snapshot as unknown as { profile: CoID<RawCoValue> }).profile,
        node,
      ).then(({ snapshot }) => {
        if (
          typeof snapshot === "object" &&
          "name" in snapshot &&
          typeof snapshot.name === "string"
        ) {
          setName(snapshot.name);
        }
      });
    }
  }, [snapshot, node, extendedType]);

  if (!snapshot) return <span>Loading...</span>;
  if (extendedType !== "account" && extendedType !== "group") {
    return <span>CoID is not an account or group</span>;
  }

  const displayName = extendedType === "account" ? name || "Account" : "Group";
  const displayText = showId ? `${displayName} (${coId})` : displayName;

  const props = onClick
    ? {
        onClick: () => onClick(displayName),
        className: classNames("text-blue-500 cursor-pointer hover:underline"),
      }
    : {
        className: classNames("text-gray-500"),
      };

  return <span {...props}>{displayText}</span>;
}
