import { CoID, LocalNode, RawCoValue } from "cojson";
import type { JsonObject } from "cojson";
import { styled } from "goober";
import { computed, ref } from "vue";
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

const PaginationContainer = styled("div")`
  padding: 1rem 0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.5rem;
`;

function CoValuesTableView({
  data,
  node,
  onNavigate,
}: {
  data: JsonObject;
  node: LocalNode;
  onNavigate: (pages: PageInfo[]) => void;
}) {
  const visibleRowsCount = ref(10);
  const coIdArrayAndVisibleRows = computed(() => {
    const coIdArray = Array.isArray(data)
      ? data
      : Object.values(data).every((k) => typeof k === "string" && isCoId(k))
        ? Object.values(data).map((k) => k as CoID<RawCoValue>)
        : [];

    const visibleRows = coIdArray.slice(0, visibleRowsCount.value);

    return [coIdArray, visibleRows];
  });

  const coIdArray = computed(() => coIdArrayAndVisibleRows.value[0]);
  const visibleRows = computed(() => coIdArrayAndVisibleRows.value[1]);
  const resolvedRows = useResolvedCoValues(visibleRows.value || [], node);

  const hasMore = computed(
    () => visibleRowsCount.value < (coIdArray.value?.length || 0),
  );

  if (!coIdArray.value?.length) {
    return <div>No data to display</div>;
  }

  if (resolvedRows.value.length === 0) {
    return <div>Loading...</div>;
  }

  const keys = Array.from(
    new Set(
      resolvedRows.value.flatMap((item: any) =>
        Object.keys(item.snapshot || {}),
      ),
    ),
  );

  const loadMore = () => {
    visibleRowsCount.value += 10;
  };

  return (
    <>
      <Table>
        <TableHead>
          <TableRow>
            {[...keys, "Action"].map((key) => (
              <TableHeader key={String(key)}>{String(key)}</TableHeader>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {(resolvedRows.value as any[])
            .slice(0, visibleRowsCount.value)
            .map((item: any, index: number) => (
              <TableRow key={index}>
                {keys.map((key) => (
                  <TableCell key={String(key)}>
                    <ValueRenderer
                      json={(item.snapshot as JsonObject)[key as string]}
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
              </TableRow>
            ))}
        </TableBody>
      </Table>
      <PaginationContainer>
        <Text muted small>
          Showing{" "}
          {Math.min(visibleRowsCount.value, coIdArray.value?.length || 0)} of{" "}
          {coIdArray.value?.length || 0}
        </Text>
        {hasMore.value && (
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
}: {
  data: JsonObject;
  node: LocalNode;
  onNavigate: (pages: PageInfo[]) => void;
}) {
  const isListOfCoValues = computed(() => {
    return Array.isArray(data) && data.every((k) => isCoId(k));
  });

  // if data is a list of covalue ids, we need to resolve those covalues
  if (isListOfCoValues.value) {
    return (
      <CoValuesTableView data={data} node={node} onNavigate={onNavigate} />
    );
  }

  // if data is a list of primitives, we can render those values directly
  return (
    <Table>
      <TableHead>
        <TableRow>
          <TableHeader style={{ width: "5rem" }}>Index</TableHeader>
          <TableHeader>Value</TableHeader>
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
            </TableRow>
          ))}
      </TableBody>
    </Table>
  );
}
