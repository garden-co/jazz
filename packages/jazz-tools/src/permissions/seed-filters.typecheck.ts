import {
  createSessionContext,
  type PermissionRelation,
  type ReachableSeedBuilder,
} from "./index.js";

export function checkSeedFilterPublicApi(seed: ReachableSeedBuilder): void {
  const session = createSessionContext();
  const legacy: PermissionRelation = seed.seeded_by(
    "members",
    "user",
    session.claims["sub"],
    "team",
  );
  const empty: PermissionRelation = seed.seeded_by(
    "members",
    "user",
    session.claims["sub"],
    "team",
    {},
  );
  const omitted: PermissionRelation = seed.seeded_by(
    "members",
    "user",
    session.claims["sub"],
    "team",
    undefined,
  );
  const filtered: PermissionRelation = seed.seeded_by(
    "members",
    "user",
    session.claims["sub"],
    "team",
    {
      active: false,
      account: { isNull: false },
      owner: session.user,
    },
  );
  // @ts-expect-error A seed principal must remain a branded session reference.
  seed.seeded_by("members", "user", "unverified-subject", "team");
  // @ts-expect-error The optional fifth argument is a record, not a scalar boolean.
  seed.seeded_by("members", "user", session.claims["sub"], "team", false);
  // @ts-expect-error Top-level null is not the public filter-record contract.
  seed.seeded_by("members", "user", session.claims["sub"], "team", null);
  // @ts-expect-error An array is not a record of column filters.
  seed.seeded_by("members", "user", session.claims["sub"], "team", []);
  void [legacy, empty, omitted, filtered];
}
