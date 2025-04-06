import { CoID, LocalNode, RawCoValue } from "cojson";
import { JsonObject, JsonValue } from "cojson";
import { ResolveIcon } from "./type-icon.js";
import { PageInfo, isCoId } from "./types.js";
import { CoMapPreview, ValueRenderer } from "./value-renderer.js";

import { Badge } from "../ui/badge.js";
import { Card, CardBody, CardHeader } from "../ui/card.js";
import { Grid } from "../ui/grid.js";
import { Text } from "../ui/text.js";

function GridItem({
  entry,
  onNavigate,
  node,
}: {
  entry: [string, JsonValue | undefined];
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
}) {
  const [key, value] = entry;
  const isCoValue = isCoId(value);

  const props = isCoValue
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
    <Card {...props}>
      <CardHeader>
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
}: {
  data: JsonObject;
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
}) {
  const entries = Object.entries(data);

  return (
    <Grid cols={entries.length === 1 ? 1 : 3}>
      {entries.map((entry, childIndex) => (
        <GridItem
          entry={entry}
          onNavigate={onNavigate}
          node={node}
          key={childIndex}
        />
      ))}
    </Grid>
  );
}
