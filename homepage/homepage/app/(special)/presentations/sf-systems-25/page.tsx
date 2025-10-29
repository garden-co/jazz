import { Slides } from "@/components/Slides";
import { JazzLogo } from "@garden-co/design-system/src/components/atoms/logos/JazzLogo";
import { GcmpLogo } from "@garden-co/design-system/src/components/atoms/logos/GcmpLogo";
import { JazzSyncs } from "@/components/icons/JazzSyncs";
import { DiagramAfterJazz } from "@/components/DiagramAfterJazz";

import sfSystemsImg from "./sf_systems_club.avif";
import { SessionEntry } from "./helpers";
import { CoValueCoreDiagram } from "./diagrams";
import { EffectiveTransactionsSlide } from "./slides/EffectiveTransactionsSlide";

export const scenario1Timestamps = [
  new Date("2025-10-29T22:00:00Z"),
  new Date("2025-10-29T22:01:00Z"),
  new Date("2025-10-29T22:02:00Z"),
  new Date("2025-10-29T22:03:00Z"),
  new Date("2025-10-29T22:04:00Z"),
  new Date("2025-10-29T22:05:00Z"),
  new Date("2025-10-29T22:06:00Z"),
];

export const header = {
  type: "comap",
  owner: "co_zCCymDTETFr2rv9U",
  createdAt: new Date("2025-10-29T22:00:00Z").toLocaleString(),
  uniqueness: "fc89fjwo3",
};

export const scenario1 = {
  alice_session_1: [
    {
      payload: { op: "set" as const, key: "color", value: "red" },
      t: scenario1Timestamps[1],
    } satisfies SessionEntry,
    {
      payload: { op: "set" as const, key: "height", value: 17 },
      t: scenario1Timestamps[4],
    } satisfies SessionEntry,
  ],
  bob_session_1: [
    {
      payload: { op: "set" as const, key: "color", value: "amber" },
      t: scenario1Timestamps[2],
    } satisfies SessionEntry,
    {
      payload: { op: "set" as const, key: "color", value: "bleen" },
      t: scenario1Timestamps[5],
    } satisfies SessionEntry,
    {
      payload: { op: "set" as const, key: "color", value: "green" },
      t: scenario1Timestamps[6],
    } satisfies SessionEntry,
  ],
  bob_session_2: [
    {
      payload: { op: "set" as const, key: "height", value: 18 },
      t: scenario1Timestamps[3],
    },
  ],
};

export default function Page() {
  return (
    <div className="flex h-screen w-full flex-col items-center justify-start gap-5 bg-black p-5 text-white">
      <Slides>
        <IntroSlide />
        <AltTitleSlide />
        <JustJazzLogoSlide />
        <SloganSlide />
        <div className="flex scale-[150%] transform origin-top flex-col gap-5">
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
        <EffectiveTransactionsSlide />
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


