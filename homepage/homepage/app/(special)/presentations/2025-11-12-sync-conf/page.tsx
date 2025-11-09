import { JazzLogo } from "@/components/forMdx";
import { IntroSlide } from "../shared/slides/IntroSlide";
import { Slides } from "@/components/Slides";
import { SimpleCentered } from "../shared/slides/Containers";
import { NewSyncDiagram } from "./NewSyncDiagram";
import { DiagramBeforeJazz } from "@/components/DiagramBeforeJazz";
import { EffectiveTransactionsSlide } from "../2025-10-29-sf-systems/slides/EffectiveTransactionsSlide";
import { scenario1 } from "../shared/scenarios";
import { CoValueSyncDiagram } from "../shared/coValueDiagrams/coValueSyncDiagram";
import { HashAndSignatureSlide } from "../2025-10-29-sf-systems/HashAndSignatureSlide";
import { CoValueCoreDiagram } from "../shared/coValueDiagrams/diagrams";

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
        <SimpleCentered>
          <DiagramBeforeJazz className="scale-[150%]" />
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            PART I: HOW IT STARTED
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Figma.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Notion.
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
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Rich Hickey cooked with Datomic
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            We're taking "keep all history" to a terminal degree.
          </h2>
        </SimpleCentered>
        <EffectiveTransactionsSlide
          scenario={scenario1}
          timestampIdx={0}
          showCore={true}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          scenario={scenario1}
          timestampIdx={1}
          showCore={true}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          scenario={scenario1}
          timestampIdx={2}
          showCore={true}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          scenario={scenario1}
          timestampIdx={3}
          showCore={true}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          scenario={scenario1}
          timestampIdx={4}
          showCore={true}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          scenario={scenario1}
          timestampIdx={5}
          showCore={true}
          showEditor={true}
        />
        <EffectiveTransactionsSlide
          scenario={scenario1}
          timestampIdx={6}
          showCore={true}
          showEditor={true}
        />
        <SimpleCentered>
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
        </SimpleCentered>

        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={0}
            aliceConnection="offline"
          />
        </SimpleCentered>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={1}
            aliceConnection="offline"
          />
        </SimpleCentered>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={2}
            aliceConnection="offline"
          />
        </SimpleCentered>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={3}
            aliceConnection="offline"
          />
        </SimpleCentered>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={4}
            aliceConnection="offline"
          />
        </SimpleCentered>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={5}
            aliceConnection="offline"
          />
        </SimpleCentered>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={6}
            aliceConnection="offline"
          />
        </SimpleCentered>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={6}
            aliceConnection={1}
          />
        </SimpleCentered>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={6}
            aliceConnection={2}
          />
        </SimpleCentered>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={6}
            aliceConnection={3}
          />
        </SimpleCentered>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={6}
            aliceConnection={4}
          />
        </SimpleCentered>
        <SimpleCentered>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={6}
            aliceConnection={5}
          />
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
        <HashAndSignatureSlide progressIdx={12} />
        <div className="pt-[10vh]">
          <CoValueCoreDiagram
            header={scenario1.header}
            sessions={scenario1.sessions}
            showView={true}
            showCore={true}
            showHashAndSignature={true}
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
            CRDTs in <JazzLogo className="inline-block h-32" />
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for infinite JSON graphs”
          </h2>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs in <JazzLogo className="inline-block h-32" />
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for infinite JSON graphs
            <br />
            with permissions”
          </h2>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Global state is great, actually.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Global state isn't global enough.
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
            PART II: How it's going
          </h1>
        </SimpleCentered>
      </Slides>
    </div>
  );
}
