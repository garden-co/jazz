import { CoID, LocalNode, RawCoValue } from "cojson";
import type { JsonObject } from "cojson";
import { styled } from "goober";
import { useMemo, useState } from "react";
import { Button } from "../ui/button.js";
import { PageInfo, isCoId } from "./types.js";
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
import { Icon } from "../ui/icon.js";

const PaginationContainer = styled("div")`
  padding: 1rem 0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.5rem;
`;

const RedTooltip = styled("span")`
  position:relative; /* making the .tooltip span a container for the tooltip text */
  border-bottom:1px dashed #000; /* little indicater to indicate it's hoverable */

  &:before {
    content: attr(data-text);
    background-color: red;
    position:absolute;

    /* vertically center */
    top:50%;
    transform:translateY(-50%);

    /* move to right */
    left:100%;
    margin-left:15px; /* and add a small left margin */

    /* basic styles */
    width:200px;
    padding:10px;
    border-radius:10px;
    color: #fff;
    text-align:center;

    display:none; /* hide by default */
  }

  &:hover:before {
    display:block;
  }
`;

function CoValuesTableView({
  data,
  node,
  onNavigate,
  onRemove,
}: {
  data: JsonObject;
  node: LocalNode;
  onNavigate: (pages: PageInfo[]) => void;
  onRemove?: (index: number) => void;
}) {
  const [visibleRowsCount, setVisibleRowsCount] = useState(10);
  const [coIdArray, visibleRows] = useMemo(() => {
    const coIdArray = Array.isArray(data)
      ? data
      : Object.values(data).every((k) => typeof k === "string" && isCoId(k))
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
    new Set(
      resolvedRows
        .filter((item) => item.snapshot !== "unavailable")
        .flatMap((item) => Object.keys(item.snapshot || {})),
    ),
  );

  const loadMore = () => {
    setVisibleRowsCount((prevVisibleRows) => prevVisibleRows + 10);
  };

  return (
    <>
      <Table>
        <TableHead>
          <TableRow>
            {["ID", ...keys, "Action"].map((key) => (
              <TableHeader key={key}>{key}</TableHeader>
            ))}
            {onRemove && <TableHeader></TableHeader>}
          </TableRow>
        </TableHead>
        <TableBody>
          {resolvedRows.slice(0, visibleRowsCount).map((item, index) => (
            <TableRow key={index}>
              <TableCell>
                <Text mono>
                  {item.snapshot === "unavailable" ? (
                    <RedTooltip data-text="Unavailable">
                      <Icon
                        name="caution"
                        color="red"
                        style={{
                          display: "inline-block",
                          marginRight: "0.5rem",
                        }}
                      />
                      {visibleRows[index]}
                    </RedTooltip>
                  ) : (
                    visibleRows[index]
                  )}
                </Text>
              </TableCell>
              {keys.map((key) => (
                <TableCell key={key}>
                  {item.snapshot !== "unavailable" && (
                    <ValueRenderer
                      json={item.snapshot[key]}
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
                  )}
                </TableCell>
              ))}

              <TableCell>
                <Button
                  variant="secondary"
                  onClick={() =>
                    onNavigate([
                      {
                        coId: item.value!.id,
                        name: index.toString(),
                      },
                    ])
                  }
                >
                  View
                </Button>
              </TableCell>
              {onRemove && (
                <TableCell>
                  <Button variant="secondary" onClick={() => onRemove(index)}>
                    Remove
                  </Button>
                </TableCell>
              )}
            </TableRow>
          ))}
        </TableBody>
      </Table>
      <PaginationContainer>
        <Text muted small>
          Showing {Math.min(visibleRowsCount, coIdArray.length)} of{" "}
          {coIdArray.length}
        </Text>
        {hasMore && (
          <Button variant="secondary" onClick={loadMore}>
            Load more
          </Button>
        )}
      </PaginationContainer>
    </>
  );
}

export function TableView({
  data,
  node,
  onNavigate,
  onRemove,
}: {
  data: JsonObject;
  node: LocalNode;
  onNavigate: (pages: PageInfo[]) => void;
  onRemove?: (index: number) => void;
}) {
  const isListOfCoValues = useMemo(() => {
    return Array.isArray(data) && data.every((k) => isCoId(k));
  }, [data]);

  // if data is a list of covalue ids, we need to resolve those covalues
  if (isListOfCoValues) {
    return (
      <CoValuesTableView
        data={data}
        node={node}
        onNavigate={onNavigate}
        onRemove={onRemove}
      />
    );
  }

  // if data is a list of primitives, we can render those values directly
  return (
    <Table>
      <TableHead>
        <TableRow>
          <TableHeader style={{ width: "5rem" }}>Index</TableHeader>
          <TableHeader>Value</TableHeader>
          {onRemove && <TableHeader>Action</TableHeader>}
        </TableRow>
      </TableHead>
      <TableBody>
        {Array.isArray(data) &&
          data?.map((value, index) => (
            <TableRow key={index}>
              <TableCell>
                <Text mono>{index}</Text>
              </TableCell>
              <TableCell>
                <ValueRenderer json={value} />
              </TableCell>
              {onRemove && (
                <TableCell>
                  <Button variant="secondary" onClick={() => onRemove(index)}>
                    Remove
                  </Button>
                </TableCell>
              )}
            </TableRow>
          ))}
      </TableBody>
    </Table>
  );
}
