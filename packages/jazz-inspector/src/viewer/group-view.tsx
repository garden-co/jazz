import { JsonObject, LocalNode, RawAccount } from "cojson";
import { CoID } from "cojson";
import { Button } from "../ui/button.js";
import { Card, CardBody, CardHeader } from "../ui/card.js";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../ui/table.js";
import { Text } from "../ui/text.js";
import { AccountNameDisplay } from "./account-name-display.js";
import { PageInfo, isCoId } from "./types.js";
import { ValueRenderer } from "./value-renderer.js";

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
                  <Button
                    variant="link"
                    onClick={() => {
                      onNavigate([{ coId: key, name: key }]);
                    }}
                  >
                    <AccountNameDisplay
                      accountId={key as CoID<RawAccount>}
                      node={node}
                    />
                  </Button>
                </TableCell>
                <TableCell>{value as string}</TableCell>
              </TableRow>
            ) : null,
          )}
        </TableBody>
      </Table>

      <Card>
        <CardHeader>
          <Text strong>Raw data</Text>
        </CardHeader>
        <CardBody>
          <ValueRenderer json={data} />
        </CardBody>
      </Card>
    </>
  );
}
