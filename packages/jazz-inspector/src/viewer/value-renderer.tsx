import { CoID, JsonValue, LocalNode, RawCoValue } from "cojson";
import { styled } from "goober";
import React, { useState } from "react";
import { Button } from "../ui/button.js";
import { Icon } from "../ui/icon.js";
import { Text } from "../ui/text.js";
import { isCoId } from "./types.js";
import { isBrowserImage, useResolvedCoValue } from "./use-resolve-covalue.js";

const LinkContainer = styled("span")`
  display: inline-flex;
  gap: 0.25rem;
  align-items: center;
`;

const BooleanText = styled("span")<{ value: boolean }>`
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  ${(props) =>
    props.value
      ? `
    color: var(--j-success-color);
  `
      : `
    color: var(--j-destructive-color);
  `}
`;

const ObjectContent = styled("pre")`
  margin-top: 0.375rem;
  font-size: 0.875rem;
  white-space: pre-wrap;
`;

const PreviewContainer = styled("div")`
  font-size: 0.875rem;
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  align-items: flex-start;
`;

const PreviewGrid = styled("div")`
  display: grid;
  grid-template-columns: auto 1fr;
  gap: 0.5rem;
`;

const PreviewMoreText = styled(Text)`
  text-align: left;
  margin-top: 0.5rem;
`;

const ImagePreviewContainer = styled("div")`
  display: flex;
  flex-direction: column;
  align-items: flex-start;
`;

const PreviewImage = styled("img")`
  width: 2rem;
  height: 2rem;
  border: 2px solid white;
  box-shadow: var(--j-shadow-sm);
  margin: 0.5rem 0;
`;

const RecordText = styled("div")`
  display: flex;
  align-items: center;
  gap: 0.25rem;
`;

const ListText = styled("div")`
  display: flex;
  align-items: center;
  gap: 0.25rem;
`;

// Is there a chance we can pass the actual CoValue here?
export function ValueRenderer({
  json,
  onCoIDClick,
  compact,
}: {
  json: JsonValue | undefined;
  onCoIDClick?: (childNode: CoID<RawCoValue>) => void;
  compact?: boolean;
}) {
  const [isExpanded, setIsExpanded] = useState(false);

  if (typeof json === "undefined" || json === undefined) {
    return <Text muted>undefined</Text>;
  }

  if (json === null) {
    return <Text muted>null</Text>;
  }

  if (typeof json === "string" && isCoId(json)) {
    const content = (
      <>
        {json}
        {onCoIDClick && <Icon name="link" />}
      </>
    );

    if (onCoIDClick) {
      return (
        <Button
          variant="link"
          onClick={() => {
            onCoIDClick?.(json as CoID<RawCoValue>);
          }}
        >
          {content}
        </Button>
      );
    }

    return <LinkContainer>{content}</LinkContainer>;
  }

  if (typeof json === "string") {
    return <Text>{json}</Text>;
  }

  if (typeof json === "number") {
    return <Text mono>{json}</Text>;
  }

  if (typeof json === "boolean") {
    return <BooleanText value={json}>{json.toString()}</BooleanText>;
  }
  const longJson = JSON.stringify(json, null, 2);
  const shortJson = longJson
    .split("\n")
    .slice(0, compact ? 3 : 8)
    .join("\n");

  // Check if collapsed differs from full
  const hasDifference = longJson !== shortJson;

  if (typeof json === "object") {
    return (
      <>
        <p>{Array.isArray(json) ? <>Array ({json.length})</> : <>Object</>}</p>
        <ObjectContent>
          {isExpanded ? longJson : shortJson}

          {hasDifference && !isExpanded ? "\n ..." : null}
        </ObjectContent>

        {!compact && hasDifference ? (
          <Button variant="link" onClick={() => setIsExpanded(!isExpanded)}>
            {isExpanded ? "Show less" : "Show more"}
          </Button>
        ) : null}
      </>
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
        style={{
          borderRadius: "0.25rem",
          backgroundColor: "var(--j-foreground)",
          animation: "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite",
          whiteSpace: "pre",
          width: "6rem",
        }}
      >
        {" "}
      </div>
    );
  }

  if (snapshot === "unavailable" && !value) {
    return (
      <Text inline muted>
        Unavailable
      </Text>
    );
  }

  if (extendedType === "image" && isBrowserImage(snapshot)) {
    return (
      <ImagePreviewContainer>
        <PreviewImage src={snapshot.placeholderDataURL} />
        <Text inline small muted>
          {snapshot.originalSize[0]} x {snapshot.originalSize[1]}
        </Text>
      </ImagePreviewContainer>
    );
  }

  if (extendedType === "record") {
    return (
      <RecordText>
        Record{" "}
        <Text inline muted>
          ({Object.keys(snapshot).length})
        </Text>
      </RecordText>
    );
  }

  if (type === "colist") {
    return (
      <ListText>
        List{" "}
        <Text inline muted>
          ({(snapshot as unknown as []).length})
        </Text>
      </ListText>
    );
  }

  const properties = Object.entries(snapshot);
  const limitedProperties =
    extendedType === "account"
      ? properties
          .filter(
            ([key]) =>
              !key.startsWith("key_z") &&
              !key.startsWith("sealer_z") &&
              key !== "readKey",
          )
          .slice(0, limit)
      : properties.slice(0, limit);

  return (
    <PreviewContainer>
      <PreviewGrid>
        {limitedProperties.map(([key, value]) => (
          <React.Fragment key={key}>
            <Text strong>{key}: </Text>
            <ValueRenderer compact json={value} />
          </React.Fragment>
        ))}
      </PreviewGrid>
      {properties.length > limit && (
        <PreviewMoreText muted small>
          {properties.length - limit} more
        </PreviewMoreText>
      )}
    </PreviewContainer>
  );
};
