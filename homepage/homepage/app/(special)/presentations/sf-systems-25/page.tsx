import { Slides } from "@/components/Slides";
import { JazzLogo } from "@garden-co/design-system/src/components/atoms/logos/JazzLogo";
import { GcmpLogo } from "@garden-co/design-system/src/components/atoms/logos/GcmpLogo";
import { JazzSyncs } from "@/components/icons/JazzSyncs";
import { DiagramAfterJazz } from "@/components/DiagramAfterJazz";

import sfSystemsImg from "./sf_systems_club.avif";
import { SessionEntry, userColors } from "./helpers";
import { CoValueCoreDiagram } from "./diagrams";
import { EffectiveTransactionsSlide } from "./slides/EffectiveTransactionsSlide";
import { HashAndSignatureSlide } from "./HashAndSignatureSlide";
import { CodeTabs } from "@/components/home/CodeTabs";

import CoMapSchemaCode from "./slides/coMapSchema.mdx";
import Alice1Code from "./slides/alice1.mdx";
import Alice2Code from "./slides/alice2.mdx";
import Bob3Code from "./slides/bob_3.mdx";
import Bob4Code from "./slides/bob_4.mdx";
import Alice5Code from "./slides/alice5.mdx";
import Bob6Code from "./slides/bob_6.mdx";
import Bob7Code from "./slides/bob_7.mdx";
import { DiagramBeforeJazz } from "@/components/DiagramBeforeJazz";

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
        <JustJazzLogoSlide />
        <SloganSlide />
        <div className="flex h-screen flex-col justify-center gap-5">
          <DiagramBeforeJazz className="scale-[150%]" />
        </div>
        <div className="flex h-screen flex-col justify-center gap-5">
          <DiagramAfterJazz className="scale-[150%]" />
        </div>
        <div className="flex h-screen flex-col justify-center gap-5">
          <div className="w-[60vw] scale-[110%]">
            <CodeTabs />
          </div>
        </div>
        <AltTitleSlide />
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
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for JSON”
          </h2>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs in <JazzLogo className="h-32 inline-block"/>
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for infinite JSON graphs”
          </h2>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs in <JazzLogo className="h-32 inline-block"/>
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for infinite JSON graphs<br/>with permissions”
          </h2>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            <span className={userColors["alice"]}>Alice</span> and{" "}
            <span className={userColors["bob"]}>Bob</span>{" "}
            are editing <br />a shared{" "}
            <span className="font-mono text-blue-500">Collaborative Map</span>
          </h1>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            <span className={userColors["alice"]}>Alice</span> and{" "}
            <span className={userColors["bob"]}>Bob</span>{" "}
            are editing <br />a shared{" "}
            <span className="font-mono text-blue-500">CoMap</span>
          </h1>
        </div>
        <div className="flex h-screen flex-col justify-center gap-5">
          <div className="w-[30vw] scale-[110%]">
            <div className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925">
              <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
                chairSchema.ts
              </span>
              <pre className="whitespace-pre-wrap break-words p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:break-words">
                <CoMapSchemaCode />
              </pre>
            </div>
          </div>
        </div>
        <EffectiveTransactionsSlide
          timestampIdx={0}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice1Code />,
            },
          ]}
        />
        <EffectiveTransactionsSlide
          timestampIdx={0}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice2Code />,
            },
          ]}
        />
        <EffectiveTransactionsSlide
          timestampIdx={1}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice2Code />,
            },
          ]}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          timestampIdx={1}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice2Code />,
            },
            {
              fileName: "bobDevice1.ts",
              code: <Bob3Code />,
            },
          ]}
        />
        <EffectiveTransactionsSlide
          timestampIdx={2}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice2Code />,
            },
            {
              fileName: "bobDevice1.ts",
              code: <Bob3Code />,
            },
          ]}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          timestampIdx={2}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice2Code />,
            },
            {
              fileName: "bobDevice1.ts",
              code: <Bob3Code />,
            },
            {
              fileName: "bobDevice2.ts",
              code: <Bob4Code />,
            },
          ]}
        />
        <EffectiveTransactionsSlide
          timestampIdx={3}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice2Code />,
            },
            {
              fileName: "bobDevice1.ts",
              code: <Bob3Code />,
            },
            {
              fileName: "bobDevice2.ts",
              code: <Bob4Code />,
            },
          ]}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          timestampIdx={3}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice5Code />,
            },
            {
              fileName: "bobDevice1.ts",
              code: <Bob3Code />,
            },
            {
              fileName: "bobDevice2.ts",
              code: <Bob4Code />,
            },
          ]}
        />
        <EffectiveTransactionsSlide
          timestampIdx={4}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice5Code />,
            },
            {
              fileName: "bobDevice1.ts",
              code: <Bob3Code />,
            },
            {
              fileName: "bobDevice2.ts",
              code: <Bob4Code />,
            },
          ]}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          timestampIdx={4}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice5Code />,
            },
            {
              fileName: "bobDevice1.ts",
              code: <Bob6Code />,
            },
            {
              fileName: "bobDevice2.ts",
              code: <Bob4Code />,
            },
          ]}
        />
        <EffectiveTransactionsSlide
          timestampIdx={5}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice5Code />,
            },
            {
              fileName: "bobDevice1.ts",
              code: <Bob6Code />,
            },
            {
              fileName: "bobDevice2.ts",
              code: <Bob4Code />,
            },
          ]}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          timestampIdx={5}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice5Code />,
            },
            {
              fileName: "bobDevice1.ts",
              code: <Bob7Code />,
            },
            {
              fileName: "bobDevice2.ts",
              code: <Bob4Code />,
            },
          ]}
        />
        <EffectiveTransactionsSlide
          timestampIdx={6}
          showCore={false}
          codeStep={[
            {
              fileName: "alice.ts",
              code: <Alice5Code />,
            },
            {
              fileName: "bobDevice1.ts",
              code: <Bob7Code />,
            },
            {
              fileName: "bobDevice2.ts",
              code: <Bob4Code />,
            },
          ]}
          showEditor={true}
        />

        <EffectiveTransactionsSlide
          timestampIdx={0}
          showCore={true}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          timestampIdx={1}
          showCore={true}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          timestampIdx={2}
          showCore={true}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          timestampIdx={3}
          showCore={true}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          timestampIdx={4}
          showCore={true}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          timestampIdx={5}
          showCore={true}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          timestampIdx={6}
          showCore={true}
          showEditor={true}
        />
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Sync
          </h1>
        </div>
        <EffectiveTransactionsSlide
          timestampIdx={6}
          showCore={true}
          showEditor={false}
        />
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Permissions
          </h1>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            Write Permissions
          </h1>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            Write Permissions
            <br />
            (Hashing & Signatures)
          </h1>
        </div>
        <HashAndSignatureSlide progressIdx={0} />
        <HashAndSignatureSlide progressIdx={1} />
        <HashAndSignatureSlide progressIdx={2} />
        <HashAndSignatureSlide progressIdx={3} />
        <HashAndSignatureSlide progressIdx={4} />
        <HashAndSignatureSlide progressIdx={5} />
        <HashAndSignatureSlide progressIdx={6} />
        <HashAndSignatureSlide progressIdx={7} />
        <HashAndSignatureSlide progressIdx={8} />
        <HashAndSignatureSlide progressIdx={9} />
        <HashAndSignatureSlide progressIdx={10} />
        <HashAndSignatureSlide progressIdx={11} />
        <HashAndSignatureSlide progressIdx={12} />
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            Write Permissions ✅<br />
            (Hashing & Signatures)
          </h1>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            Read Permissions
            <br />
            (Encryption)
          </h1>
        </div>
        <HashAndSignatureSlide progressIdx={12} />
        <div className="pt-[10vh]">
          <CoValueCoreDiagram
            header={header}
            sessions={scenario1}
            showView={true}
            showCore={true}
            showHashAndSignature={true}
            encryptedItems={true}
          />
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            Who can read & write?
            <br />
            Which encryption key to use?
          </h1>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            Orchestrating Permissions
          </h1>
        </div>
        <div className="pt-[10vh]">
          <CoValueCoreDiagram
            header={header}
            sessions={scenario1}
            showView={true}
            showCore={true}
            showHashAndSignature={true}
            encryptedItems={true}
          />
        </div>
        <div className="pt-[10vh]">
          <CoValueCoreDiagram
            header={header}
            sessions={scenario1}
            showView={true}
            showCore={true}
            showHashAndSignature={true}
            encryptedItems={true}
            highlightOwner={true}
          />
        </div>
        <div className="pt-[10vh]">
          <CoValueCoreDiagram
            header={header}
            sessions={scenario1}
            showView={true}
            showCore={true}
            showHashAndSignature={true}
            encryptedItems={true}
            highlightOwner={true}
            group={{
              roles: {
                alice: "admin",
                bob: "writer",
              },
              currentKey: "keyID_z89fdhd9",
            }}
          />
        </div>
        <div className="pt-[10vh]">
          <CoValueCoreDiagram
            header={header}
            sessions={scenario1}
            showView={true}
            showCore={true}
            showHashAndSignature={true}
            encryptedItems={true}
            highlightOwner={true}
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
        <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            Read & Write Permissions ✅<br />
            (Hashing & Signatures & Encryption,<br/>Orchestrated by Groups)
          </h1>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-10 p-20 pl-[20vw]">
          <h1 className="font-display text-6xl font-semibold tracking-tight">
            Much more to <JazzLogo className="inline-block h-20" />
          </h1>
          <ul className="list-disc font-display text-4xl font-semibold flex flex-col gap-4">
            <li className="ml-10">Other CoValue types:<br/>CoList, CoPlainText & CoRichText, FileStream</li>
            <li className="ml-10">CoValues referencing each other:<br/>JSON-like trees/graphs<br/>...that can be granularly loaded & subscribed to!</li>
            <li className="ml-10">Groups that have other groups as members<br/>
            ...to form complex permission hierarchies!</li>
            <li className="ml-10">Global infrastructure for sync & storage</li>
          </ul>
        </div>
        <div className="flex h-screen w-screen flex-col justify-center gap-10 p-20 pl-[20vw]">
          <h1 className="font-display text-6xl font-semibold tracking-tight">
            Currently solving
          </h1>
          <ul className="list-disc font-display text-4xl font-semibold flex flex-col gap-4">
            <li className="ml-10">Compression and no-history CoMaps</li>
            <li className="ml-10">Indices & queries</li>
            <li className="ml-10">Granular global transactions</li>
            <li className="ml-10">Scaling our infra 10,000x</li>
            <li className="ml-10">DSL for more expressive permissions</li>
          </ul>
        </div>
        <FollowJazzSlide />
        <ThanksSlide />
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

      <div className="-my-20 flex items-center justify-between">
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
          className="w-[50%] opacity-50 invert"
        />
      </div>

      <div className="relative z-10 flex items-center justify-between">
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

function FollowJazzSlide() {
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

      <div className="-my-20 flex items-center justify-between">
        <h1 className="z-10 font-display text-8xl font-semibold tracking-tight">
        <a
            href="https://jazz.tools"
            target="_blank"
            rel="noopener noreferrer"
          >
            https://jazz.tools
          </a>
          <br />
          <a
            href="https://x.com/jazz_tools"
            target="_blank"
            rel="noopener noreferrer"
          >
            @jazz_tools
          </a>
          <br />
          <a
            href="https://x.com/anselm_io"
            target="_blank"
            rel="noopener noreferrer"
          >
            @anselm_io
          </a>
        </h1>

        <img
          src={sfSystemsImg.src}
          alt="SF Systems Meetup"
          className="right-0 w-[50%] opacity-50 invert"
        />
      </div>

      <div className="relative z-10 flex items-center justify-between">
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


function ThanksSlide() {
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

      <div className="-my-20 flex items-center justify-between">
        <h1 className="z-10 font-display text-8xl font-semibold tracking-tight">
          Thank you!
        </h1>

        <img
          src={sfSystemsImg.src}
          alt="SF Systems Meetup"
          className="right-0 w-[50%] opacity-50 invert"
        />
      </div>

      <div className="relative z-10 flex items-center justify-between">
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
