import bs58 from "bs58";
import clsx from "clsx";
import { BinaryIcon, SignatureIcon } from "lucide-react";
import { Slides } from "@/components/Slides";
import { JazzLogo } from "@garden-co/design-system/src/components/atoms/logos/JazzLogo";
import { GcmpLogo } from "@garden-co/design-system/src/components/atoms/logos/GcmpLogo";
import { JazzSyncs } from "@/components/icons/JazzSyncs";
import { DiagramAfterJazz } from "@/components/DiagramAfterJazz";

import sfSystemsImg from "./sf_systems_club.avif";

type SessionEntry = {
  payload: { op: "set"; key: string; value?: string | number } & object;
  t: Date;
};

export default function Page() {
  const scenario1 = {
    alice_session_1: [
      {
        payload: { op: "set" as const, key: "color", value: "red" },
        t: new Date(Date.now() - 10 * 60 * 1000),
      } satisfies SessionEntry,
      {
        payload: { op: "set" as const, key: "height", value: 17 },
        t: new Date(Date.now() - 3 * 60 * 1000),
      } satisfies SessionEntry,
    ],
    bob_session_1: [
      {
        payload: { op: "set" as const, key: "color", value: "amber" },
        t: new Date(Date.now() - 3 * 60 * 1000),
      } satisfies SessionEntry,
      {
        payload: { op: "set" as const, key: "color", value: "bleen" },
        t: new Date(Date.now() - 2 * 60 * 1000),
      } satisfies SessionEntry,
      {
        payload: { op: "set" as const, key: "color", value: "green" },
        t: new Date(Date.now() - 1 * 60 * 1000),
      } satisfies SessionEntry,
    ],
    bob_session_2: [
      {
        payload: { op: "set" as const, key: "height", value: 18 },
        t: new Date(Date.now() - 4 * 60 * 1000),
      },
    ],
  };

  const header = {
    type: "comap",
    owner: "co_zCCymDTETFr2rv9U",
    createdAt: "2024-12-06...",
    uniqueness: "fc89fjwo3",
  };

  return (
    <div className="flex h-screen w-full flex-col items-center justify-center gap-5 bg-black p-5 text-white">
      <Slides>
        <IntroSlide />
        <AltTitleSlide />
        <JustJazzLogoSlide />
        <SloganSlide />
        <div className="flex scale-[150%] flex-col gap-5">
          <DiagramAfterJazz className="h-[60%]" />
          <div className="flex justify-between gap-10">
            <p>Offline-first local storage that looks like useState()</p>
            <p>
              VPS/bare-metal: embedded DB (like SQLite) + ORM
              <br />
              Serverless: embedded in-memory cache + ORM
            </p>
          </div>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Conflict-free
            <br />
            Replicated
            <br />
            Data Types
          </h1>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs
          </h1>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “
            <a
              href="http://archagon.net/blog/2018/03/24/data-laced-with-history/"
              target="_blank"
              rel="noopener noreferrer"
              className="underline"
            >
              Data laced with history
            </a>
            ”
          </h2>
        </div>
        <div>
          <p>Basic setup</p>
          <CoValueCoreDiagram
            header={header}
            sessions={scenario1}
            showView={false}
            showHashAndSignature={false}
            encryptedItems={false}
          />
        </div>
        <div>
          <p>Showing latest (effective) transactions per key</p>
          <CoValueCoreDiagram
            header={header}
            sessions={scenario1}
            showView={true}
            showHashAndSignature={false}
            encryptedItems={false}
          />
        </div>
        <div>
          {" "}
          <p>Showing hash and signature</p>
          <CoValueCoreDiagram
            header={header}
            sessions={scenario1}
            showView={true}
            showHashAndSignature={true}
            encryptedItems={false}
          />
        </div>
        <div>
          {" "}
          <p>Showing encrypted items</p>
          <CoValueCoreDiagram
            header={header}
            sessions={scenario1}
            showView={true}
            showHashAndSignature={true}
            encryptedItems={true}
          />
        </div>
        <div>
          {" "}
          <p>Showing group</p>
          <CoValueCoreDiagram
            header={header}
            sessions={scenario1}
            showView={true}
            showHashAndSignature={true}
            encryptedItems={true}
            group={{
              roles: {
                alice: "admin",
                bob: "writer",
              },
              currentKey: "keyID_z89fdhd9",
            }}
          />
        </div>
        <div>
          {" "}
          <p>Showing extended group</p>
          <CoValueCoreDiagram
            header={header}
            sessions={scenario1}
            showView={true}
            showHashAndSignature={true}
            encryptedItems={true}
            group={{
              roles: {
                alice: "admin",
                bob: "writer",
              },
              currentKey: "keyID_z89fdhd9",
            }}
            showFullGroup={true}
          />
        </div>
      </Slides>
    </div>
  );
}

function IntroSlide() {
  return (
    <div className="flex h-screen w-screen flex-col justify-between gap-5 p-20">
      <div className="flex justify-between">
        <JazzLogo className="h-12 self-start" />
        <div className="relative z-10 text-right">
          <a
            href="https://jazz.tools"
            target="_blank"
            rel="noopener noreferrer"
          >
            jazz.tools
          </a>
          <br />
          <a
            href="https://x.com/jazz_tools"
            target="_blank"
            rel="noopener noreferrer"
          >
            @jazz_tools
          </a>
        </div>
      </div>

      <div className="relative">
        <h1 className="relative z-10 font-display text-8xl font-semibold tracking-tight">
          Cryptographic
          <br />
          Permissions for <br />
          Conflict-free
          <br />
          Replicated
          <br />
          Data Types
        </h1>

        <img
          src={sfSystemsImg.src}
          alt="SF Systems Meetup"
          className="absolute -bottom-[25%] right-0 h-[170%] opacity-50 invert"
        />
      </div>

      <div className="flex items-center justify-between">
        <GcmpLogo className="h-12" />
        <div className="text-center">
          Anselm Eickhoff
          <br />
          <a
            href="https://x.com/anselm_io"
            target="_blank"
            rel="noopener noreferrer"
          >
            @anselm_io
          </a>
        </div>
        <h2 className="text-right">
          SF Systems Meetup
          <br />
          October '25
        </h2>
      </div>
    </div>
  );
}

function AltTitleSlide() {
  return (
    <div className="flex h-screen w-screen flex-col items-center justify-center gap-5 p-20">
      <h2 className="font-display text-5xl font-semibold tracking-tight">
        “How you could have invented <JazzLogo className="inline-block h-16" />{" "}
        in an afternoon”
      </h2>
    </div>
  );
}

function JustJazzLogoSlide() {
  return (
    <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
      <JazzLogo className="h-20" />
    </div>
  );
}

function SloganSlide() {
  return (
    <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
      <JazzSyncs className="h-40" />
    </div>
  );
}

const encoder = new TextEncoder();

function CoValueCoreDiagram({
  header,
  sessions,
  showView,
  showHashAndSignature,
  encryptedItems,
  group,
  showFullGroup,
}: {
  header: object;
  sessions: {
    [key: `${string}_session_${string}`]: SessionEntry[];
  };
  showView: boolean;
  showHashAndSignature: boolean;
  encryptedItems: boolean;
  group?: {
    roles: { [user: string]: "reader" | "writer" | "admin" };
    currentKey: string;
  };
  showFullGroup?: boolean;
}) {
  return (
    <div
      className={clsx("grid gap-10 bg-black p-10", {
        "grid-cols-2": group,
      })}
    >
      {showView && (
        <div className="col-span-1">
          <ContentView
            sessions={sessions}
            header={header}
            highlightSpecial={false}
          />
        </div>
      )}
      {group && (
        <div className="col-span-1">
          <ContentView
            header={headerForGroup(group)}
            sessions={sessionsForGroup(group)}
            highlightSpecial={true}
          />
        </div>
      )}

      <div className="col-span-1">
        <CoValueCoreView
          header={header}
          sessions={sessions}
          showView={showView}
          showHashAndSignature={showHashAndSignature}
          encryptedItems={encryptedItems}
        />
      </div>
      {group && showFullGroup && (
        <div className="col-span-1 origin-top scale-75">
          <CoValueCoreView
            header={headerForGroup(group)}
            sessions={sessionsForGroup(group)}
            showView={false}
            showHashAndSignature={false}
            encryptedItems={false}
          />
        </div>
      )}
    </div>
  );
}

function HeaderContent({ header }: { header: object }) {
  return (
    <div className="relative h-full rounded-lg bg-stone-800 px-4 py-3">
      <div className="mb-2 flex justify-between text-stone-500">header</div>
      <pre className="text-sm leading-6 text-white">
        {JSON.stringify(header, null, 2)
          .replace(/"(.+?)":/g, "$1:")
          .replace(/\n\s+/g, "\n")
          .replace(/,/g, "")
          .replace(/[{}]\n?/g, "")}
      </pre>
      <div className="absolute right-3 top-1 py-2 text-sm">
        h(header) = {fakeCoID(header)} ("ID")
      </div>
    </div>
  );
}

function headerForGroup(group: {
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

function sessionsForGroup(group: {
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

function SessionHeader({ sessionKey }: { sessionKey: string }) {
  return (
    <div className="min-w-[5.5rem] items-baseline rounded-lg bg-stone-900 px-3 py-2">
      <span
        className={clsx([
          userColors[sessionKey.split("_")[0]],
          "font-semibold",
        ])}
      >
        {sessionKey.split("_")[0]}
      </span>{" "}
      <span className="text-sm">
        {sessionKey.split("_").slice(1).join(" ")}
      </span>
    </div>
  );
}

function CoValueCoreView({
  header,
  sessions,
  showView,
  showHashAndSignature,
  encryptedItems,
}: {
  header: object;
  sessions: {
    [key: string]: SessionEntry[];
  };
  showView: boolean;
  showHashAndSignature: boolean;
  encryptedItems: boolean;
}) {
  return (
    <div className="not-prose relative flex flex-col gap-5">
      <div className="min-w-[17rem] flex-1">
        <HeaderContent header={header} />
      </div>
      <div className="flex flex-[6] gap-5">
        {Object.entries(sessions).map(([sessionID, log]) => (
          <div key={sessionID} className="flex min-w-48 flex-1 flex-col gap-1">
            <SessionHeader sessionKey={sessionID} />
            {log.map((item, idx) => {
              return (
                <TransactionContainer
                  key={JSON.stringify(item)}
                  sessions={sessions}
                  item={item}
                  idx={idx}
                  log={log}
                  showView={showView}
                >
                  <TransactionIndexMarker index={idx} />
                  <TransactionContent
                    item={item}
                    encryptedItems={encryptedItems}
                  />
                  <Timestamp timestamp={item.t} />
                  {showHashAndSignature && <HashChainArrow />}
                </TransactionContainer>
              );
            })}
            {showHashAndSignature && (
              <HashAndSignature log={log} sessionID={sessionID} />
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

const userColors: { [user: string]: string } = {
  alice: "text-emerald-500",
  bob: "text-amber-500",
};

function fakeHash(session: { payload: object; t: Date }[]) {
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

function fakeCoID(header: object) {
  return (
    "co_z" + bs58.encode(encoder.encode(hashCode(JSON.stringify(header)) + ""))
  );
}

function fakeSignature(session: { payload: object; t: Date }[]) {
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

function hashCode(str: string) {
  let hash = 0;
  for (let i = 0, len = str.length; i < len; i++) {
    let chr = str.charCodeAt(i);
    hash = (hash << 5) - hash + chr;
    hash |= 0; // Convert to 32bit integer
  }
  return hash;
}

function fakeEncryptedPayload(payload: object) {
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

function ContentView({
  sessions,
  header,
  highlightSpecial,
}: {
  sessions: { [key: string]: SessionEntry[] };
  header: object;
  highlightSpecial: boolean;
}) {
  const lastEntries = Object.values(sessions)
    .flatMap((session) => session)
    .reduce(
      (state, entry) => {
        if ((state[entry.payload.key]?.t.getTime() || 0) < entry.t.getTime()) {
          state = { ...state, [entry.payload.key]: entry };
        }
        return state;
      },
      {} as Record<string, SessionEntry>,
    );

  const pairs = Object.entries(lastEntries)
    .map(
      ([key, entry]) => [key, entry.payload.value] as [string, string | number],
    )
    .sort((a, b) =>
      a[0].startsWith("keyID")
        ? 1
        : b[0].startsWith("keyID")
          ? -1
          : a[0].localeCompare(b[0]),
    );

  return (
    <div className="relative m-10 flex min-w-48 flex-col gap-1 self-center rounded-lg border-2 border-blue-500 font-mono text-blue-500">
      <div className="absolute -top-5 text-xs text-blue-500">
        {(header as any).isGroup ? "Group" : "CoMap"} {fakeCoID(header)}
      </div>
      {pairs.map(([key, value], idx) => (
        <div
          key={key}
          className={clsx("px-2 py-1", {
            "border-b border-blue-500": idx !== pairs.length - 1,
          })}
        >
          {highlightSpecialString(key)}: {highlightSpecialString(value)}
        </div>
      ))}
    </div>
  );
}

function highlightSpecialString(key: string | number) {
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

function HashChainArrow() {
  return (
    <div className="absolute -bottom-7 -left-2 z-10 h-[100%]">
      <ArrowSvg className="h-[100%]" />
      <div className="absolute -left-8 top-[50%] -mt-[30%] bg-black text-[0.6rem]">
        blake3
      </div>
    </div>
  );
}

function ArrowSvg({ className }: { className?: string }) {
  return (
    <svg
      width="18"
      height="76"
      viewBox="0 0 18 76"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
    >
      <path
        d="M17.2368 73.8253C17.7733 73.6945 18.1023 73.1535 17.9716 72.617L15.8409 63.8728C15.7101 63.3362 15.1691 63.0072 14.6326 63.138C14.096 63.2687 13.767 63.8097 13.8977 64.3463L15.7917 72.1189L8.01912 74.0128C7.48253 74.1436 7.15354 74.6846 7.28429 75.2212C7.41504 75.7577 7.95602 76.0867 8.49261 75.956L17.2368 73.8253ZM17 0.853699C16.4804 -0.000710453 16.4798 -0.000368717 16.4792 1.0481e-05C16.4789 0.000186638 16.4783 0.000604156 16.4777 0.00095712C16.4765 0.00166352 16.4751 0.0025206 16.4735 0.00352878C16.4702 0.00554512 16.466 0.00816592 16.4608 0.0113945C16.4504 0.0178515 16.4362 0.0267399 16.4183 0.0380862C16.3825 0.0607783 16.332 0.0933053 16.2678 0.135879C16.1394 0.221022 15.9559 0.346381 15.7253 0.513653C15.2642 0.848155 14.6142 1.35056 13.8386 2.03447C12.2876 3.40209 10.2322 5.49745 8.18068 8.42921C4.07083 14.3025 0 23.5006 0 36.8537H1H2C2 23.9371 5.92918 15.1352 9.81933 9.57587C11.7678 6.79135 13.7124 4.81229 15.1614 3.53457C15.8858 2.8958 16.4858 2.43279 16.8997 2.13255C17.1066 1.98246 17.2669 1.87311 17.3728 1.8029C17.4258 1.76779 17.4651 1.74247 17.4899 1.72674C17.5023 1.71887 17.5111 1.71341 17.5161 1.71031C17.5186 1.70876 17.5201 1.7078 17.5207 1.70744C17.521 1.70726 17.5211 1.70722 17.5209 1.70733C17.5208 1.70739 17.5205 1.70758 17.5204 1.70761C17.52 1.70784 17.5196 1.70811 17 0.853699ZM1 36.8537H0C0 50.2068 4.07083 59.4049 8.18068 65.2782C10.2322 68.2099 12.2876 70.3053 13.8386 71.6729C14.6142 72.3568 15.2642 72.8592 15.7253 73.1937C15.9559 73.361 16.1394 73.4864 16.2678 73.5715C16.332 73.6141 16.3825 73.6466 16.4183 73.6693C16.4362 73.6807 16.4504 73.6895 16.4608 73.696C16.466 73.6992 16.4702 73.7019 16.4735 73.7039C16.4751 73.7049 16.4765 73.7057 16.4777 73.7064C16.4783 73.7068 16.4789 73.7072 16.4792 73.7074C16.4798 73.7078 16.4804 73.7081 17 72.8537C17.5196 71.9993 17.52 71.9996 17.5204 71.9998C17.5205 71.9998 17.5208 72 17.5209 72.0001C17.5211 72.0002 17.521 72.0001 17.5207 72C17.5201 71.9996 17.5186 71.9986 17.5161 71.9971C17.5111 71.994 17.5023 71.9885 17.4899 71.9807C17.4651 71.9649 17.4258 71.9396 17.3728 71.9045C17.2669 71.8343 17.1066 71.7249 16.8997 71.5748C16.4858 71.2746 15.8858 70.8116 15.1614 70.1728C13.7124 68.8951 11.7678 66.916 9.81933 64.1315C5.92918 58.5722 2 49.7703 2 36.8537H1Z"
        fill="white"
      />
    </svg>
  );
}

function HashAndSignature({
  log,
  sessionID,
}: {
  log: SessionEntry[];
  sessionID: string;
}) {
  return (
    <div className="-mt-px min-w-[9.5rem] justify-start rounded p-2">
      <pre className="flex items-center gap-1 text-sm text-white">
        <BinaryIcon className="h-4 w-4" /> {fakeHash(log)}
      </pre>
      <pre
        className={clsx(
          "flex items-center gap-1 text-sm",
          userColors[sessionID.split("_")[0] as keyof typeof userColors],
        )}
      >
        <SignatureIcon className="h-4 w-4" />
        {fakeSignature(log)}
      </pre>
    </div>
  );
}

function TransactionContainer({
  children,
  sessions,
  item,
  idx,
  log,
  showView,
}: {
  children: React.ReactNode;
  sessions: { [key: string]: SessionEntry[] };
  item: SessionEntry;
  idx: number;
  log: SessionEntry[];
  showView: boolean;
}) {
  const isLastPerKey =
    showView &&
    item.t.getTime() >=
      Object.values(sessions)
        .flatMap((session) => session)
        .filter((i) => i.payload.key === item.payload.key)
        .reduce((max, item) => Math.max(max, item.t.getTime()), 0);
  return (
    <div
      key={JSON.stringify(item)}
      className={clsx(
        "relative min-w-[9rem] bg-stone-800",
        isLastPerKey ? "outline outline-blue-500" : "",
        {
          "mt-1.5 rounded-t-lg": idx === 0,
          "mb-1.5 rounded-b-lg": idx === log.length - 1,
        },
      )}
    >
      {children}
    </div>
  );
}

function TransactionContent({
  item,
  encryptedItems,
}: {
  item: SessionEntry;
  encryptedItems: boolean;
}) {
  return encryptedItems ? (
    <pre className="px-3 py-2 text-sm leading-6 text-fuchsia-500">
      {fakeEncryptedPayload(item.payload)}
    </pre>
  ) : (
    <pre className="px-3 pt-2 text-sm leading-6 text-white">
      {item.payload.op === "set"
        ? `${item.payload.key}: ${item.payload.value}`
        : `${item.payload.key}: deleted`}
    </pre>
  );
}

function Timestamp({ timestamp }: { timestamp: Date }) {
  return (
    <div className="-mt-3 flex justify-between gap-2 px-2 pb-1">
      <pre className="ml-auto text-[0.6rem] font-semibold">
        {timestamp.toLocaleString("en-US", {
          hour: "numeric",
          minute: "2-digit",
        })}
      </pre>
    </div>
  );
}

function TransactionIndexMarker({ index }: { index: number }) {
  return (
    <pre className="absolute -left-3 top-1/2 -translate-y-1/2 text-xs text-stone-500">
      {index}
    </pre>
  );
}
