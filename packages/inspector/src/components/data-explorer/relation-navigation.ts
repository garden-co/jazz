export function buildRelationFilterSearch(id: string): { filters: string } {
  return {
    filters: JSON.stringify([{ id: `relation-id-${id}`, column: "id", operator: "eq", value: id }]),
  };
}
