import { CoID, LocalNode, RawCoValue } from "cojson";
import { JsonObject, JsonValue } from "cojson";
import { useState } from "react";
import { styled } from "goober";
import { ResolveIcon } from "./type-icon.js";
import { PageInfo, isCoId } from "./types.js";
import { CoMapPreview, ValueRenderer } from "./value-renderer.js";
import { CoValueEditor } from "./co-value-editor.js";

import { Badge } from "../ui/badge.js";
import { Card, CardBody, CardHeader } from "../ui/card.js";
import { Grid } from "../ui/grid.js";
import { Icon } from "../ui/icon.js";
import { Text } from "../ui/text.js";
import { isWriter } from "../utils/permissions.js";

function GridItem({
  entry,
  onNavigate,
  node,
  coValue,
}: {
  entry: [string, JsonValue | undefined];
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
  coValue?: RawCoValue;
}) {
  const [key, value] = entry;
  const isCoValue = isCoId(value);
  const [isEditing, setIsEditing] = useState(false);

  const handleEditClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    setIsEditing(true);
  };

  const handleCancel = () => {
    setIsEditing(false);
  };

  const handleDelete = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (confirm(`Are you sure you want to delete the property "${key}"?`)) {
      coValue?.core.makeTransaction(
        [
          {
            op: "del",
            key,
          },
        ],
        "private",
      );
    }
  };

  if (isEditing) {
    return (
      <Card
        style={{
          backgroundColor: "var(--j-foreground)",
          borderColor: "var(--j-foreground)",
        }}
      >
        <CardHeader>
          <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
            {isCoValue ? (
              <>
                <Text strong>{key}</Text>
                <Badge>
                  <ResolveIcon coId={value as CoID<RawCoValue>} node={node} />
                </Badge>
              </>
            ) : (
              <Text strong>{key}</Text>
            )}
          </div>
        </CardHeader>
        <CardBody style={{ wordBreak: "break-word" }}>
          <CoValueEditor
            node={node}
            property={key}
            value={value}
            coValue={coValue!}
            onCancel={handleCancel}
          />
        </CardBody>
      </Card>
    );
  }

  const cardProps = isCoValue
    ? {
        onClick: () =>
          onNavigate([{ coId: value as CoID<RawCoValue>, name: key }]),
        as: "button",
      }
    : {
        style: {
          backgroundColor: "var(--j-foreground)",
          borderColor: "var(--j-foreground)",
        },
      };

  return (
    <Card {...cardProps}>
      <CardHeader>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          {isCoValue ? (
            <>
              <Text strong>{key}</Text>
              <Badge>
                <ResolveIcon coId={value as CoID<RawCoValue>} node={node} />
              </Badge>
            </>
          ) : (
            <Text strong>{key}</Text>
          )}
        </div>
        {coValue && isWriter(coValue.group.myRole()) && (
          <ActionButtons>
            <EditButton
              onClick={handleEditClick}
              type="button"
              aria-label="Edit"
            >
              <Icon name="edit" size="sm" />
            </EditButton>
            <DeleteButton
              onClick={handleDelete}
              type="button"
              aria-label="Delete"
            >
              <Icon name="delete" size="sm" />
            </DeleteButton>
          </ActionButtons>
        )}
      </CardHeader>
      <CardBody style={{ wordBreak: "break-word" }}>
        {isCoValue ? (
          <CoMapPreview coId={value as CoID<RawCoValue>} node={node} />
        ) : (
          <ValueRenderer
            json={value}
            onCoIDClick={(coId) => {
              onNavigate([{ coId, name: key }]);
            }}
          />
        )}
      </CardBody>
    </Card>
  );
}

export function GridView({
  data,
  onNavigate,
  node,
  coValue,
}: {
  data: JsonObject;
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
  coValue?: RawCoValue;
}) {
  const entries = Object.entries(data);

  return (
    <Grid cols={entries.length === 1 ? 1 : 3}>
      {entries.map((entry, childIndex) => (
        <GridItem
          entry={entry}
          onNavigate={onNavigate}
          node={node}
          coValue={coValue}
          key={childIndex}
        />
      ))}
    </Grid>
  );
}

const EditButton = styled("button")`
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 0.25rem;
  border: none;
  background: transparent;
  cursor: pointer;
  color: var(--j-text-color);
  border-radius: var(--j-radius-sm);
  transition: background-color 0.2s;

  &:hover {
    background-color: var(--j-foreground);
  }
`;

const DeleteButton = styled("button")`
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 0.25rem;
  border: none;
  background: transparent;
  cursor: pointer;
  color: var(--j-text-color);
  border-radius: var(--j-radius-sm);
  transition: background-color 0.2s;

  &:hover {
    background-color: var(--j-foreground);
  }
`;

const ActionButtons = styled("div")`
  display: flex;
  align-items: center;
  gap: 0.25rem;
`;
