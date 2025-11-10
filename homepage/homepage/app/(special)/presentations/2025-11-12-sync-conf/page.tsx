import { JazzLogo } from "@/components/forMdx";
import { IntroSlide } from "../shared/slides/IntroSlide";
import { Slides } from "@/components/Slides";
import { SimpleCentered } from "../shared/slides/Containers";
import { NewSyncDiagram } from "./NewSyncDiagram";
import { DiagramBeforeJazz } from "@/components/DiagramBeforeJazz";
import { EffectiveTransactionsSlide } from "../shared/slides/EffectiveTransactionsSlide";
import { scenario1 } from "../shared/scenarios";
import { CoValueSyncDiagram } from "../shared/coValueDiagrams/coValueSyncDiagram";
import { HashAndSignatureSlide } from "../shared/slides/HashAndSignatureSlide";
import { CoValueCoreDiagram } from "../shared/coValueDiagrams/diagrams";
import {
  HowYouCouldHaveInventedJazz,
  JustJazzLogoSlide,
  SloganSlide,
} from "../shared/slides/slogans";
import { userColors } from "../shared/coValueDiagrams/helpers";
import ChairCoMapSchemaCode from "../shared/slides/chairCoMapSchema.mdx";
import ChairEditingCode from "../shared/slides/chairEditing.mdx";
import ChairComponentCode from "../shared/slides/chairComponent.mdx";
import ChairLoadCode from "../shared/slides/chairLoad.mdx";
import ChairSubscriptionCode from "../shared/slides/chairSubscription.mdx";

import saasMinesImg from "./slides/saas_mines.png";
import richHiickeyImg from "./slides/rich_hickey.png";
import monkeyAk47Img from "./slides/monkey_ak47.png";

export default function Page() {
  return (
    <div className="flex h-[100dvh] w-full flex-col items-center justify-start gap-5 bg-black text-white">
      <Slides>
        <IntroSlide
          talkTitle={
            <>
              Oops, my
              <br />
              sync engine
              <br />
              has become
              <br />a database.
            </>
          }
          eventName="Sync Conf"
          eventDate="November '25"
        />
        <JustJazzLogoSlide />
        <SloganSlide />
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            <span className="text-[2.35em] font-extralight">PART 0</span>
            <br />
            How I got radicalized
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <img src={saasMinesImg.src} alt="Saas Mines" className="w-[70%] mx-auto" />
        </SimpleCentered>

        <SimpleCentered>
          <DiagramBeforeJazz className="scale-[150%]" />
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Figma & Notion
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Ink & Switch.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            <span className="text-[2.35em] font-extralight">PART I</span>
            <br />
            How it started
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Automerge.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Conflict-free
            <br />
            Replicated
            <br />
            Data Types
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for JSON”
          </h2>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            What an object is:
            <br />
            its edit history.
          </h2>
        </SimpleCentered>
        <SimpleCentered>
          <img src={richHiickeyImg.src} alt="Rich Hickey" className="w-[70%] mx-auto" />
        </SimpleCentered>
        <HowYouCouldHaveInventedJazz />
        <SimpleCentered>
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            <span className={userColors["alice"]}>Alice</span> and{" "}
            <span className={userColors["bob"]}>Bob</span> are <br/>designing a
            chair.
          </h1>
        </SimpleCentered>
        <div className="flex h-screen flex-col justify-center gap-5">
          <div className="w-[30vw] scale-[200%]">
            <div className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925">
              <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
                chairSchema.ts
              </span>
              <pre className="whitespace-pre-wrap break-words p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:break-words">
                <ChairCoMapSchemaCode />
              </pre>
            </div>
          </div>
        </div>
        <div className="flex h-screen flex-col justify-center gap-5">
          <div className="w-[30vw] scale-[200%]">
            <div className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925">
              <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
                creationAndEditing.ts
              </span>
              <pre className="whitespace-pre-wrap break-words p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:break-words">
                <ChairEditingCode />
              </pre>
            </div>
          </div>
        </div>
        <div className="flex h-screen flex-col justify-center gap-5">
          <div className="w-[30vw] scale-[200%]">
            <div className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925">
              <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
                chairComponent.tsx
              </span>
              <pre className="whitespace-pre-wrap break-words p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:break-words">
                <ChairComponentCode />
              </pre>
            </div>
          </div>
        </div>
        <div className="flex h-screen flex-col justify-center gap-5">
          <div className="w-[30vw] scale-[200%]">
            <div className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925">
              <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
                chairServer.ts
              </span>
              <pre className="whitespace-pre-wrap break-words p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:break-words">
                <ChairLoadCode />
              </pre>
            </div>
          </div>
        </div>
        <div className="flex h-screen flex-col justify-center gap-5">
          <div className="w-[30vw] scale-[200%]">
            <div className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925">
              <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
                chairSubscription.ts
              </span>
              <pre className="whitespace-pre-wrap break-words p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:break-words">
                <ChairSubscriptionCode />
              </pre>
            </div>
          </div>
        </div>
        <SimpleCentered>
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            <span className={userColors["alice"]}>Alice</span> and{" "}
            <span className={userColors["bob"]}>Bob</span> are <br/>designing a
            chair.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            <span className={userColors["alice"]}>Alice</span> and{" "}
            <span className={userColors["bob"]}>Bob</span> are editing <br />a
            shared <span className="font-mono text-blue-500">co.map</span>
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            <span className={userColors["alice"]}>Alice</span> and{" "}
            <span className={userColors["bob"]}>Bob</span> are editing <br />a
            shared{" "}
            <span className="font-mono text-blue-500">collaborative map</span>
          </h1>
        </SimpleCentered>
        {...Array.from({ length: scenario1.timestamps.length }).map((_, timestampIdx) => (
          <EffectiveTransactionsSlide
            key={timestampIdx}
            scenario={scenario1}
            timestampIdx={timestampIdx}
            showCore={true}
            showEditor={true}
          />
        ))}
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Maybe: CoList
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for JSON”
          </h2>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CoValues in <JazzLogo className="inline-block h-32" />
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for infinite JSON graphs”
          </h2>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            TODO: Refs & Resolve Queries
          </h1>
        </SimpleCentered>
        {/* <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            But isn't that slow?
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            LWW goes BRRRRRRRRR
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            But doesn't it take a lot of space?
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Compression helps a lot.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            TODO: CoList
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            But aren't linked lists slow?
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Common case of linear inserts goes BRRRRRRRRR
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            But doesn't it take a lot of space?
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Successor/Predecessor pointers compress well.
          </h1>
        </SimpleCentered> */}

        {...Array.from({ length: scenario1.timestamps.length }).map((_, timestampIdx) => (
          <SimpleCentered key={timestampIdx}>
            <CoValueSyncDiagram
              scenario={scenario1}
              timestampIdx={timestampIdx}
              aliceConnection="offline"
            />
          </SimpleCentered>
        ))}
        {...Array.from({ length: 5 }).map((_, aliceProgress) => (
          <SimpleCentered key={aliceProgress}>
            <CoValueSyncDiagram
              scenario={scenario1}
              timestampIdx={scenario1.timestamps.length - 1}
              aliceConnection={aliceProgress + 1}
            />
          </SimpleCentered>
        ))}

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CoValues = Sparkling CRDTs from the Champagne Region of France?
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Permissions
          </h1>
        </SimpleCentered>

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
        <HashAndSignatureSlide progressIdx={13} />
        <SimpleCentered>
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            Write Permissions ✅<br />
            (Hashing & Signatures)
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            Read Permissions
            <br />
            (Encryption)
          </h1>
        </SimpleCentered>
        <HashAndSignatureSlide progressIdx={13} />
        <div className="pt-[10vh]">
          <CoValueCoreDiagram
            header={scenario1.header}
            sessions={scenario1.sessions}
            showView={true}
            showCore={true}
            showHashAndSignature={true}
            hashProgressIdx={13}
            encryptedItems={true}
          />
        </div>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={6}
            aliceConnection={5}
            serverEncrypted={false}
          />
        </SimpleCentered>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={6}
            aliceConnection={5}
            serverEncrypted={true}
          />
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CoValues in <JazzLogo className="inline-block h-32" />
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for infinite JSON graphs”
          </h2>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CoValues in <JazzLogo className="inline-block h-32" />
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for infinite JSON graphs
            <br />
            with permissions”
          </h2>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            1) Everything is multiplayer.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            2) Everything is offline-first.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            3) Sync is all you need.
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            <span className="line-through">PlanetScale</span> Space Scale.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Apocalypse-first.
          </h1>
        </SimpleCentered>


        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Amazing DX<br/>
            Amazing UX
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Local-first conf 2024
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <img src={monkeyAk47Img.src} alt="Monkey Ak47" className="w-[70%] mx-auto" />
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            PART II: How it's going
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            What people have been building
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            PART III: What's next
          </h1>
        </SimpleCentered>

        <SimpleCentered>
        <NewSyncDiagram />
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Durable Objects ain't it.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Pact with the devil: eventual consistency.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <table>
            <thead>
              <tr>
                <th> </th>
                <th>Traditional DB</th>
                <th>DB + sync-engine</th>
                <th>Durable Objects</th>
                <th>Jazz</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <th>Read Proximity</th>
                <td>us-east-1</td>
                <td>close to each user</td>
                <td>avg of users in DO</td>
                <td>close to each user</td>
              </tr>
              <tr>
                <th>Write Proximity</th>
                <td>us-east-1</td>
                <td>us-east-1</td>
                <td>avg of users in DO</td>
                <td>close to each user</td>
              </tr>
              <tr>
                <th>Offline-first</th>
                <td>no</td>
                <td>optimisitc writes</td>
                <td>no</td>
                <td>true local writes</td>
              </tr>
              <tr>
                <th>Realtime/Multiplayer/LLM streaming</th>
                <td>slow</td>
                <td>depends</td>
                <td>fast</td>
                <td>fast</td>
              </tr>
              <tr>
                <th>Consistency</th>
                <td>strong</td>
                <td>strong + optimistic</td>
                <td>strong</td>
                <td>eventual</td>
              </tr>
            </tbody>
          </table>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Don't need to isolate compute
          </h1>
        </SimpleCentered>
      </Slides>
    </div>
  );
}
