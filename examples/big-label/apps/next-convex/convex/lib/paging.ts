/**
 * One page of an ordered index range, as the Jazz app's
 * `orderBy(...).limit(pageSize + 1).offset(n)`: skips `offset` matching rows
 * and returns up to `limit`. The extra row tells the pager there is a next
 * page. Rows are read in index order, so a filter (search, status) stops
 * reading as soon as the page is full.
 */
export async function pageOf<T>(
  rows: AsyncIterable<T>,
  { offset, limit }: { offset: number; limit: number },
  matches: (row: T) => boolean = () => true,
) {
  const page: T[] = [];
  let skipped = 0;
  for await (const row of rows) {
    if (!matches(row)) continue;
    if (skipped < offset) {
      skipped++;
      continue;
    }
    page.push(row);
    if (page.length >= limit) break;
  }
  return page;
}

