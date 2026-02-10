import { CoID, LocalNode, RawCoStream, RawCoValue } from "cojson";
import { CoStreamItem } from "cojson";
import type { JsonObject, JsonValue } from "cojson";
import { styled } from "goober";
import { AccountOrGroupText } from "./account-or-group-text.js";
import { PageInfo } from "./types.js";

const CoStreamGrid = styled("div")`
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 0.5rem;
`;

const CoStreamItemContainer = styled("div")`
  padding: 0.75rem;
  border-radius: var(--j-radius-lg);
  overflow: hidden;
  border: 1px solid rgb(229 231 235);
  cursor: pointer;
  box-shadow: var(--j-shadow-sm);
  &:hover {
    background-color: rgb(243 244 246 / 0.05);
  }
`;

function RenderCoStream({
  value,
  node,
}: {
  value: RawCoStream;
  node: LocalNode;
}) {
  const streamPerUser = Object.keys(value.items);
  const userCoIds = streamPerUser.map((stream) => stream.split("_session")[0]);

  return (
    <CoStreamGrid>
      {userCoIds.map((id, idx) => (
        <CoStreamItemContainer key={id}>
          <AccountOrGroupText coId={id as CoID<RawCoValue>} node={node} />
          {/* @ts-expect-error - TODO: fix types */}
          {value.items[streamPerUser[idx]]?.map(
            (item: CoStreamItem<JsonValue>) => (
              <div key={item.tx.txIndex + item.tx.sessionID}>
                {new Date(item.madeAt).toLocaleString()}{" "}
                {JSON.stringify(item.value)}
              </div>
            ),
          )}
        </CoStreamItemContainer>
      ))}
    </CoStreamGrid>
  );
}

export function CoStreamView({
  value,
  node,
}: {
  data: JsonObject;
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
  value: RawCoStream;
}) {
  return <RenderCoStream value={value} node={node} />;
}
