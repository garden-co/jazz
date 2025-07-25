import { JsonObject, LocalNode, RawAccount } from "cojson";
import { CoID } from "cojson";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../ui/table.js";
import { Text } from "../ui/text.js";
import { AccountOrGroupText } from "./account-or-group-text.js";
import { RawDataCard } from "./raw-data-card.js";
import { PageInfo, isCoId } from "./types.js";

export function GroupView({
  data,
  onNavigate,
  node,
}: {
  data: JsonObject;
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
}) {
  return (
    <>
      <Text strong>Members</Text>

      <Table>
        <TableHead>
          <TableRow>
            <TableHeader>Account</TableHeader>
            <TableHeader>Permission</TableHeader>
          </TableRow>
        </TableHead>
        <TableBody>
          {"everyone" in data && typeof data.everyone === "string" ? (
            <TableRow>
              <TableCell>everyone</TableCell>
              <TableCell>{data.everyone}</TableCell>
            </TableRow>
          ) : null}

          {Object.entries(data).map(([key, value]) =>
            isCoId(key) ? (
              <TableRow key={key}>
                <TableCell>
                  <AccountOrGroupText
                    coId={key as CoID<RawAccount>}
                    node={node}
                    showId
                    onClick={() => {
                      onNavigate([{ coId: key, name: key }]);
                    }}
                  />
                </TableCell>
                <TableCell>{value as string}</TableCell>
              </TableRow>
            ) : null,
          )}
        </TableBody>
      </Table>

      <RawDataCard data={data} />
    </>
  );
}
