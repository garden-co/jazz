import { pool } from "../../../src/lib/db.server";
import { formatCatalogNumber } from "../../../src/fixtures";
import { requestAccount } from "../../../src/lib/request-account";

export const runtime = "nodejs";

/**
 * The number after the highest one used in a catalogue, e.g. NFC-013, for
 * the release form's suggestion. A one-off read for a member of the label (a
 * Route Handler, like the sealing route: Next runs Server Actions one at a time).
 */
export async function POST(request: Request) {
  const account = await requestAccount();
  if (!account) return Response.json({ error: "account required" }, { status: 401 });
  const { organizationId, catalogueId } = (await request.json().catch(() => ({}))) as Record<string, unknown>;
  if (typeof organizationId !== "string" || typeof catalogueId !== "string")
    return Response.json({ error: "invalid request" }, { status: 400 });
  const { rows } = await pool.query<{ code: string; sequence: number }>(
    `SELECT c.code, coalesce(max(r.catalog_sequence), 0)::int AS sequence
     FROM catalogues c LEFT JOIN releases r ON r.organization_id = c.organization_id AND r.catalogue_id = c.id
     WHERE c.id = $2 AND c.organization_id = $1
       AND EXISTS (SELECT 1 FROM memberships m WHERE m.organization_id = $1 AND m.user_id = $3)
     GROUP BY c.code`,
    [organizationId, catalogueId, account.accountId],
  );
  if (!rows[0]) return Response.json({ error: "not found" }, { status: 404 });
  return Response.json({ catalogNumber: formatCatalogNumber(rows[0].code, rows[0].sequence + 1) });
}
