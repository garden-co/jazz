import { CoID, LocalNode, RawCoValue } from "cojson";
import type { JsonObject } from "cojson";
import { useMemo, useState } from "react";
import { Button } from "../ui/button.js";
import { Icon } from "../ui/icon.js";
import { classNames } from "../utils.js";
import { PageInfo } from "./types.js";
import { useResolvedCoValues } from "./use-resolve-covalue.js";
import { ValueRenderer } from "./value-renderer.js";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../ui/table.js";
import { Text } from "../ui/text.js";

export function TableView({
  data,
  node,
  onNavigate,
}: {
  data: JsonObject;
  node: LocalNode;
  onNavigate: (pages: PageInfo[]) => void;
}) {
  const [visibleRowsCount, setVisibleRowsCount] = useState(10);
  const [coIdArray, visibleRows] = useMemo(() => {
    const coIdArray = Array.isArray(data)
      ? data
      : Object.values(data).every(
            (k) => typeof k === "string" && k.startsWith("co_"),
          )
        ? Object.values(data).map((k) => k as CoID<RawCoValue>)
        : [];

    const visibleRows = coIdArray.slice(0, visibleRowsCount);

    return [coIdArray, visibleRows];
  }, [data, visibleRowsCount]);
  const resolvedRows = useResolvedCoValues(visibleRows, node);

  const hasMore = visibleRowsCount < coIdArray.length;

  if (!coIdArray.length) {
    return <div>No data to display</div>;
  }

  if (resolvedRows.length === 0) {
    return <div>Loading...</div>;
  }

  const keys = Array.from(
    new Set(resolvedRows.flatMap((item) => Object.keys(item.snapshot || {}))),
  );

  const loadMore = () => {
    setVisibleRowsCount((prevVisibleRows) => prevVisibleRows + 10);
  };

  return (
    <div>
      <Table>
        <TableHead>
          <TableRow>
            {["", ...keys].map((key) => (
              <TableHeader key={key}>{key}</TableHeader>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {resolvedRows.slice(0, visibleRowsCount).map((item, index) => (
            <TableRow key={index}>
              <TableCell>
                <Button
                  variant="link"
                  onClick={() =>
                    onNavigate([
                      {
                        coId: item.value!.id,
                        name: index.toString(),
                      },
                    ])
                  }
                >
                  <Icon name="link" />
                </Button>
              </TableCell>
              {keys.map((key) => (
                <TableCell key={key}>
                  <ValueRenderer
                    json={(item.snapshot as JsonObject)[key]}
                    onCoIDClick={(coId) => {
                      async function handleClick() {
                        onNavigate([
                          {
                            coId: item.value!.id,
                            name: index.toString(),
                          },
                          {
                            coId: coId,
                            name: key,
                          },
                        ]);
                      }

                      handleClick();
                    }}
                  />
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
      <div
        className={classNames("py-4 flex items-center justify-between gap-2")}
      >
        <Text muted>
          Showing {Math.min(visibleRowsCount, coIdArray.length)} of{" "}
          {coIdArray.length}
        </Text>
        {hasMore && (
          <Button variant="secondary" onClick={loadMore}>
            Load more
          </Button>
        )}
      </div>
    </div>
  );
}
