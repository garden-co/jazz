import bs58 from "bs58";

export type SessionEntry = {
  payload: { op: "set"; key: string; value?: string | number } & object;
  t: Date;
};

export const userColors: { [user: string]: string } = {
  alice: "text-emerald-500",
  bob: "text-amber-500",
};

const encoder = new TextEncoder();

export function fakeHash(session: { payload: object; t: Date }[]) {
  return (
    "hash_z" +
    bs58.encode(
      encoder.encode(
        hashCode(
          session.reduce((acc, item) => acc + JSON.stringify(item), ""),
        ) + "",
      ),
    )
  );
}

export function fakeCoID(header: object) {
  return (
    "co_z" + bs58.encode(encoder.encode(hashCode(JSON.stringify(header)) + ""))
  );
}

export function fakeSignature(session: { payload: object; t: Date }[]) {
  return (
    "sig_x" +
    bs58.encode(
      encoder.encode(
        hashCode(
          hashCode(
            session.reduce((acc, item) => acc + JSON.stringify(item), ""),
          ) + "",
        ) + "",
      ),
    )
  );
}

export function hashCode(str: string) {
  let hash = 0;
  for (let i = 0, len = str.length; i < len; i++) {
    let chr = str.charCodeAt(i);
    hash = (hash << 5) - hash + chr;
    hash |= 0; // Convert to 32bit integer
  }
  return hash;
}

export function fakeEncryptedPayload(payload: object) {
  return (
    "encr_z" +
    bs58.encode(
      encoder.encode(hashCode(JSON.stringify(payload)) + "").slice(0, 12),
    ) +
    "…\n…" +
    bs58.encode(
      encoder.encode(hashCode(JSON.stringify(payload) + "a") + "").slice(0, 12),
    )
  );
}

export function highlightSpecialString(key: string | number) {
  const fragments = (key + "").split("_");
  return fragments.flatMap((fragment, idx) => [
    userColors[fragment] ? (
      <span key={idx} className={userColors[fragment]}>
        {fragment}
      </span>
    ) : fragments[idx - 1]?.startsWith("keyID") ? (
      <span key={idx} className="text-fuchsia-500">
        {fragment}
      </span>
    ) : (
      fragment
    ),
    idx !== fragments.length - 1 ? "_" : "",
  ]);
}

export function headerForGroup(group: {
  roles: { [user: string]: "reader" | "writer" | "admin" };
  currentKey: string;
}) {
  return {
    type: "comap",
    isGroup: true,
    owner: Object.keys(group.roles)[0],
    createdAt: "2024-12-06...",
    uniqueness: group.currentKey,
  };
}

export function sessionsForGroup(group: {
  roles: { [user: string]: "reader" | "writer" | "admin" };
  currentKey: string;
}) {
  return {
    [Object.keys(group.roles)[0] + "_session_1"]: [
      {
        payload: {
          op: "set" as const,
          key: "readKey",
          value: group.currentKey,
        },
        t: new Date(Date.now() - 10 * 60 * 1000),
      },
      ...Object.entries(group.roles).flatMap(([user, role]) => [
        {
          payload: {
            op: "set" as const,
            key: user,
            value: role,
          },
          t: new Date(Date.now() - 10 * 60 * 1000),
        },
        {
          payload: {
            op: "set" as const,
            key: group.currentKey + "_for_" + user,
            value: fakeEncryptedPayload({
              encrKey: group.currentKey + user,
            })
              .split("\n")[0]
              .replace("encr_z", "sealed_z"),
          },
          t: new Date(Date.now() - 10 * 60 * 1000),
        },
      ]),
    ],
  };
}