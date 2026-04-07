export function buildRelationFilterHref(table: string, id: string): string {
  const params = new URLSearchParams();
  params.set(
    "filters",
    JSON.stringify([{ id: `relation-id-${id}`, column: "id", operator: "eq", value: id }]),
  );
  return `/data-explorer/${table}/data?${params.toString()}`;
}
