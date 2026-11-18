import { Badge } from "@astryxdesign/core/Badge";
import type { Stock } from "@/jazz.generated";
import { LOW_STOCK } from "@/src/catalogue/catalogue";

export function StockBadge({ stock }: { stock?: Stock }) {
  if (!stock) return null;
  if (stock.onHand <= 0) return <Badge variant="error" label="Out of stock" />;
  if (stock.onHand <= LOW_STOCK)
    return <Badge variant="warning" label={`Only ${stock.onHand} left`} />;
  return <Badge variant="success" label="In stock" />;
}
