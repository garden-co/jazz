import clsx from "clsx";
import { BinaryIcon, SignatureIcon } from "lucide-react";
import {
  fakeCoID,
  fakeEncryptedPayload,
  fakeHash,
  fakeSignature,
  headerForGroup,
  highlightSpecialString,
  SessionEntry,
  sessionsForGroup,
  userColors,
} from "./helpers";
import { EditorIndicator } from "./EditorIndicator";

export function CoValueCoreDiagram({
  header,
  sessions,
  showView,
  showCore,
  showHashAndSignature,
  encryptedItems,
  group,
  showFullGroup,
  showEditor,
  currentTimestamp,
  hashProgressIdx,
  highlightOwner,
}: {
  header: object;
  sessions: {
    [key: `${string}_session_${string}`]: SessionEntry[];
  };
  showView: boolean;
  showCore: boolean;
  showHashAndSignature: boolean;
  encryptedItems: boolean;
  group?: {
    roles: { [user: string]: "reader" | "writer" | "admin" };
    currentKey: string;
  };
  showFullGroup?: boolean;
  showEditor?: boolean;
  currentTimestamp?: Date;
  hashProgressIdx?: number;
  highlightOwner?: boolean;
}) {
  return (
    <div
      className={clsx(
        "relative grid rounded-xl border-2 border-stone-200 bg-black",
        {
          "grid-cols-2": group,
        },
      )}
    >
      <div className="z-1 absolute -top-8 left-3 border border-stone-500 bg-stone-200 px-2 text-xl font-mono text-black text-bolder rounded-t-sm">
        {(header as any).isGroup ? "Group" : "CoMap"} {fakeCoID(header)}
      </div>
      {showCore && (
        <div className="col-span-1">
          <CoValueCoreView
            header={header}
            sessions={sessions}
            showView={showView}
            showHashAndSignature={showHashAndSignature}
            encryptedItems={encryptedItems}
            hashProgressIdx={hashProgressIdx}
            highlightOwner={highlightOwner}
          />
        </div>
      )}
      {group && showFullGroup && (
        <div className="col-span-1">
          <CoValueCoreView
            header={headerForGroup(group)}
            sessions={sessionsForGroup(group)}
            showView={false}
            showHashAndSignature={false}
            encryptedItems={false}
          />
        </div>
      )}
      {showView && (
        <div className="col-span-1">
          <ContentView
            sessions={sessions}
            header={header}
            showEditor={showEditor || false}
            currentTimestamp={currentTimestamp || new Date()}
          />
        </div>
      )}
      {group && (
        <div className="col-span-1">
          <ContentView
            header={headerForGroup(group)}
            sessions={sessionsForGroup(group)}
            showEditor={false}
            currentTimestamp={currentTimestamp || new Date()}
          />
        </div>
      )}
    </div>
  );
}

function HeaderContent({
  header,
  highlightOwner,
}: {
  header: object;
  highlightOwner?: boolean;
}) {
  const headerLines = JSON.stringify(header, null, 2)
    .replace(/"(.+?)":/g, "$1:")
    .replace(/\n\s+/g, "\n")
    .replace(/,/g, "")
    .replace(/[{}]\n?/g, "")
    .split("\n");
  return (
    <div className="relative h-full rounded-t-xl border-b border-stone-500 bg-stone-900 p-5">
      <div className="absolute -left-6 top-0 bottom-0 [writing-mode:sideways-lr] text-lg text-center leading-none text-stone-400 font-mono">
        HEADER
      </div>

      <pre
        className={clsx(
          "grid grid-cols-2 leading-6",
          highlightOwner ? "text-white" : "text-stone-300",
        )}
      >
        {headerLines.map((line, index) => (
          <div
            key={index}
            className={clsx(
              highlightOwner && line.startsWith("owner") ? "text-white" : "",
            )}
          >
            {line}
          </div>
        ))}
      </pre>
    </div>
  );
}

function SessionHeader({ sessionKey }: { sessionKey: string }) {
  return (
    <div className="min-w-[5.5rem] items-baseline rounded-lg text-xl -mb-2">
      <span
        className={clsx([
          userColors[sessionKey.split("_")[0]],
          "font-semibold",
        ])}
      >
        {sessionKey.split("_")[0]}
      </span>{" "}
      <span className="text-sm">
        {sessionKey.replace("session", "device").split("_").slice(1).join(" ")}
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
  hashProgressIdx,
  highlightOwner,
}: {
  header: object;
  sessions: {
    [key: string]: SessionEntry[];
  };
  showView: boolean;
  showHashAndSignature: boolean;
  encryptedItems: boolean;
  hashProgressIdx?: number;
  highlightOwner?: boolean;
}) {
  return (
    <div className="not-prose relative flex flex-col">
      <div className="flex-1">
        <HeaderContent header={header} highlightOwner={highlightOwner} />
      </div>
      <div className="bg-stone-950 relative min-h-[13em] border-b border-stone-500 p-5">
        <div className="absolute -left-6 top-0 bottom-0 [writing-mode:sideways-lr] text-lg text-center leading-none text-stone-400 font-mono ">
          HISTORY
        </div>
        <div className="grid grid-cols-3 gap-7">
          {Object.entries(sessions).map(([sessionID, log], sessionIdx) => {
            const priorHashProgress = Object.values(sessions)
              .slice(0, sessionIdx)
              .reduce((acc, session) => acc + session.length, sessionIdx * 2);

            return (
              <div
                key={sessionID}
                className="flex min-w-48 max-w-64 flex-1 flex-col gap-1"
              >
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
                      <TransactionContent
                        item={item}
                        encryptedItems={encryptedItems}
                      />
                      <Timestamp timestamp={item.t} />
                      {showHashAndSignature &&
                        (hashProgressIdx ?? Infinity) >
                          idx + priorHashProgress && <HashChainArrow />}
                    </TransactionContainer>
                  );
                })}
                {showHashAndSignature && (
                  <div className="-mt-px min-w-[9.5rem] justify-start rounded p-2">
                    {(hashProgressIdx ?? Infinity) >
                      log.length + priorHashProgress && (
                      <pre className="flex items-center gap-1 text-sm text-white">
                        <BinaryIcon className="h-4 w-4" /> {fakeHash(log)}
                      </pre>
                    )}
                    {(hashProgressIdx ?? Infinity) >
                      log.length + 1 + priorHashProgress && (
                      <div className="relative">
                        <div className="absolute -left-16 top-1 rounded bg-black px-1 text-xs">
                          ed25519
                        </div>
                        <pre
                          className={clsx(
                            "flex items-center gap-1 text-sm",
                            userColors[
                              sessionID.split("_")[0] as keyof typeof userColors
                            ],
                          )}
                        >
                          <SignatureIcon className="h-4 w-4" />
                          {fakeSignature(log)}
                        </pre>
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
        <div className="absolute -bottom-4 left-1/2 -translate-x-1/2 text-center text-stone-400 bg-black px-1 text-xl">
        ▼
        </div>
      </div>
    </div>
  );
}

function ContentView({
  sessions,
  header,
  showEditor,
  currentTimestamp,
}: {
  sessions: { [key: string]: SessionEntry[] };
  header: object;
  showEditor: boolean;
  currentTimestamp: Date;
}) {
  const lastEntries = Object.entries(sessions)
    .flatMap(([sessionID, session]) =>
      session.map((entry) => ({ ...entry, by: sessionID.split("_")[0] })),
    )
    .reduce(
      (state, entry) => {
        if ((state[entry.payload.key]?.t.getTime() || 0) < entry.t.getTime()) {
          state = { ...state, [entry.payload.key]: entry };
        }
        return state;
      },
      {} as Record<string, SessionEntry & { by: string }>,
    );

  const pairs = Object.entries(lastEntries).sort((a, b) =>
    a[0].startsWith("keyID")
      ? 1
      : b[0].startsWith("keyID")
        ? -1
        : a[0].localeCompare(b[0]),
  );

  return (
    <div className="relative flex min-h-40 min-w-48 flex-col gap-1 self-center p-5">
      <div className="absolute -left-6 top-0 bottom-0 [writing-mode:sideways-lr] text-lg text-center leading-none text-stone-400 font-mono">
        STATE
      </div>
      <div className="font-mono text-2xl text-stone-300">
        {"{"}
        {pairs.map(([key, entry], idx) => (
          <div key={key} className="ml-4 max-w-fit relative">
            {highlightSpecialString(key)}:{" "}
            {highlightSpecialString(entry.payload.value + "")}
            {showEditor && entry.t.getTime() === currentTimestamp.getTime() && (
              <EditorIndicator by={entry.by} key={JSON.stringify(entry)} />
            )}
          </div>
        ))}
        {"}"}
      </div>
    </div>
  );
}

export function HashChainArrow() {
  return (
    <div className="absolute -bottom-7 -left-2 z-10 h-[100%]">
      <ArrowSvg className="h-[100%]" />
      <div className="absolute -left-12 top-[50%] -mt-[40%] rounded bg-black px-1 text-sm">
        blake3
      </div>
    </div>
  );
}

export function ArrowSvg({ className }: { className?: string }) {
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

export function TransactionContainer({
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
        "relative flex gap-0.5 items-stretch overflow-hidden",
        isLastPerKey ? "text-white" : "text-stone-700",
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

export function TransactionContent({
  item,
  encryptedItems,
}: {
  item: SessionEntry;
  encryptedItems: boolean;
}) {
  return encryptedItems ? (
    <pre className="flex-1 overflow-hidden text-sm px-3 py-2 leading-6 text-fuchsia-500 bg-stone-900">
      {fakeEncryptedPayload(item.payload)}
    </pre>
  ) : (
    <pre className="flex-1 overflow-hidden text-xl px-3 py-2 leading-6 bg-stone-900">
      {item.payload.op === "set"
        ? `${item.payload.key}: ${item.payload.value}`
        : `${item.payload.key}: deleted`}
    </pre>
  );
}

export function Timestamp({ timestamp }: { timestamp: Date }) {
  return (
    <div className="flex justify-between items-center px-3 bg-stone-900">
      <pre className="ml-auto text-sm font-semibold">
        {timestamp.toLocaleString("en-US", {
          hour: "numeric",
          minute: "2-digit",
        })}
      </pre>
    </div>
  );
}
