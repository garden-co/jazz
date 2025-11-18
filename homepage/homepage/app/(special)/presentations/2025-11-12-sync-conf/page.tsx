import { JazzLogo } from "@/components/forMdx";
import { IntroSlide } from "../shared/slides/IntroSlide";
import { Slide, Slides } from "@/components/Slides";
import { SimpleCentered } from "../shared/slides/Containers";
import {
  DurableObjectsDiagram,
  DurableObjectsDiagram2,
  DurableObjectsDiagram3,
  EvenSimplerNewSyncDiagram,
  NewSyncDiagram,
  SimpleNewSyncDiagram,
  SimpleNewSyncDiagramIndexClient,
  SimpleNewSyncDiagramIndexWorker,
  SimpleNewSyncDiagramWithLambda,
  SimpleNewSyncDiagramWithLambdaAndRPC,
  SimpleNewSyncDiagramWithLambdaAndSSR,
  SyncEngineDiagram,
  TradDBDiagram,
} from "./NewSyncDiagram";
import { DiagramBeforeJazz } from "@/components/DiagramBeforeJazz";
import { EffectiveTransactionsSlide } from "../shared/slides/EffectiveTransactionsSlide";
import { scenario1 } from "../shared/scenarios";
import { CoValueSyncDiagram } from "../shared/coValueDiagrams/coValueSyncDiagram";
import { HashAndSignatureSlide } from "../shared/slides/HashAndSignatureSlide";
import { CoValueCoreDiagram } from "../shared/coValueDiagrams/diagrams";
import { userColors } from "../shared/coValueDiagrams/helpers";
import ChairCoMapSchemaCode from "../shared/slides/chairCoMapSchema.mdx";
import ChairEditingCode from "../shared/slides/chairEditing.mdx";
import ChairComponentCode from "../shared/slides/chairComponent.mdx";
import ChairLoadCode from "../shared/slides/chairLoad.mdx";
import ChairSubscriptionCode from "../shared/slides/chairSubscription.mdx";

import saasMinesImg from "./slides/saas_mines.png";
import monkeyAk47Img from "./slides/monkey_ak47.png";
import tradeOffer from "./slides/trade_offer.png";
import goats from "./slides/goats.png";
import crud from "./slides/crud.jpeg";
import ken from "./slides/ken.png";
import vercel from "./slides/vercel.png";
import venn from "./slides/venn.png";
import space from "./slides/space.png";
import apocalypse from "./slides/apocalypse.png";
import justSync from "./slides/just_sync.png";
import sync from "./slides/sync.png";
import mars from "./slides/mars.png";
import booth from "./slides/booth.jpg";
import syncConf from "./slides/sync_conf.svg";
import praise from "./slides/praise.jpg";
import spp from "./slides/spp.png";
import fork from "./slides/fork.png";

import { JazzSyncs } from "@/components/icons/JazzSyncs";
import {
  FeaturesSection,
  FeaturesSectionSparse,
} from "@/components/home/FeaturesSection";

export default function Page() {
  return (
    <div className="flex h-[100dvh] w-full flex-col items-center justify-start gap-5 bg-black text-white">
      <Slides>
        <Slide
          notes={[
            "Hi everyone! My name is Anselm,",
            "I run a small company called Garden Computing",
            "and we're building Jazz,",
            "which we recently learned is a database",
          ]}
        >
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
            eventDate="Nov 2025"
            image={syncConf}
          />
        </Slide>
        <Slide notes={["So the way we introduce Jazz now is..."]}>
          <JazzLogo className="h-40" />
        </Slide>
        <Slide notes={["The database that syncs"]}>
          <JazzSyncs className="h-80" />
        </Slide>
        <Slide
          notes={[
            "Let me tell you how we came to that understanding",
            "By starting from the very beginning",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            <span className="text-[2.35em] font-extralight">PART 1</span>
            <br />
            Sync-maxxing
            <br />
            and becoming
            <br />
            local-first pilled
          </h1>
        </Slide>
        <Slide
          notes={[
            "What you need to know about me",
            "...is that in my time in the B2B SaaS mines,",
            "I've seen and built a lot of apps...",
          ]}
        >
          <img
            src={saasMinesImg.src}
            alt="Saas Mines"
            className="mx-auto w-[70%] rounded-xl"
          />
        </Slide>

        <Slide
          notes={[
            "each with their own unique stack",
            "all of which sucked",
            "and so did the apps",
          ]}
        >
          <DiagramBeforeJazz className="mx-auto scale-[150%]" />
        </Slide>
        <Slide
          notes={[
            "But then in 2019 or so",
            "I learned about Figma and Notion",
            "And they sucked slightly less",
            "Because they were much more multi-player than your average app",
            "But at the same time felt more like a high-fidelity desktop app",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Figma & Notion
          </h1>
        </Slide>
        <Slide
          notes={[
            "Then the same year, I discovered Ink & Switch",
            "and the idea of local-first software",
            "and it radicalized me",
            "and I wanted to build my own local-first Notion",
            "So I started looking for tools",
            "Because I knew none of the horrible SaaS stacks",
            "would be able to handle it",
          ]}
        >
          <img src={goats.src} className="mx-auto w-[70%] rounded-2xl" />
        </Slide>
        <Slide
          notes={[
            "Luckily, it turned out that Ink & Switch also was *making* those tools",
            "so I discovered Automerge",
            "and I immediately knew that I had struck gold",
            "because at the time it was just good enough to reveal its potential",
            "and just bad enough that I got nerd-sniped into building my own thing",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Automerge.
          </h1>
        </Slide>
        <Slide
          notes={[
            "What was so exciting about Automerge was",
            "that it was the first CRDT I had ever seen",
            "and it was actually usable!",
            "and the more I played with it,",
            "the more I questioned my B2B SaaS upbringing",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs.
          </h1>
        </Slide>
        <Slide
          notes={[
            "what if web2 apps were trying to be multiplayer, but their stacks were just too clunky",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            1) What if every app wants to be real-time multiplayer?
          </h1>
        </Slide>
        <Slide
          notes={[
            "or what if every app could work offline and be as snappy as local state",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            2) What if every app could be offline-first?
          </h1>
        </Slide>
        <Slide
          notes={[
            "what if I didn't need to think about networking and requests at all",
            "and sync just took care of it? What if sync was all you need?",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            3) What if sync is all you need?
          </h1>
        </Slide>
        <Slide
          notes={[
            "That's when I knew that I was all in on CRDTs",
            "So let me take you on a quick journey",
            "And visually show you what I saw in CRDTs",
            "And how I made them work in Jazz",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs.
          </h1>
        </Slide>
        <Slide
          notes={[
            "Now, CRDTs have a somewhat descriptive name",
            "Conflict-free replicated datatypes",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Conflict-free
            <br />
            Replicated
            <br />
            Data Types
          </h1>
        </Slide>
        <Slide
          notes={[
            "But my mental shortcut for that",
            "is that they're just Git for JSON",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for JSON”
          </h2>
        </Slide>
        <Slide
          notes={[
            "And the main shift in mindset is that",
            "what an object really is",
            "is not its current mutable state",
            "but the full history of edits that ever happened to it",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            What an object is:
            <br />
            <span className="line-through">its current mutable state</span>
            <br />
            its full edit history.
          </h2>
        </Slide>
        <Slide
          notes={[
            "what does that look like?",
            "Let's do an example",
            "Let's say Alice and Bob are designing a chair",
          ]}
        >
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            <span className={userColors["alice"]}>Alice</span> and{" "}
            <span className={userColors["bob"]}>Bob</span> are <br />
            designing a chair.
          </h1>
        </Slide>
        <Slide
          notes={[
            "In Jazz, we would start kinda like this",
            "We've got a chair schema with height and color attributes",
          ]}
        >
          <div className="mx-auto w-[30vw] scale-[200%]">
            <div className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925">
              <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
                chairSchema.ts
              </span>
              <pre className="whitespace-pre-wrap break-words p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:break-words">
                <ChairCoMapSchemaCode />
              </pre>
            </div>
          </div>
        </Slide>
        <Slide
          notes={[
            "We can then go ahead and create a new chair, locally",
            "read from it and modify it synchronously,",
            "as if it was a normal local object",
            "but what's also happening in the background here",
            "is that the chair is being synced to the cloud and to other users",
            "and every change persists locally and in the cloud",
          ]}
        >
          <div className="mx-auto w-[30vw] scale-[200%]">
            <div className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925">
              <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
                creationAndEditing.ts
              </span>
              <pre className="whitespace-pre-wrap break-words p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:break-words">
                <ChairEditingCode />
              </pre>
            </div>
          </div>
        </Slide>
        <Slide
          notes={[
            "We can then go ahead and use the chair as reactive state",
            "where useCoState makes sure that every time the chair changes,",
            "we re-render this component",
            "but unlike plain useState, this also rerenders on remote changes",
            "whenever other users edit this chair",
            "and their edits get synced in",
          ]}
        >
          <div className="mx-auto w-[30vw] scale-[200%]">
            <div className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925">
              <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
                chairComponent.tsx
              </span>
              <pre className="whitespace-pre-wrap break-words p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:break-words">
                <ChairComponentCode />
              </pre>
            </div>
          </div>
        </Slide>
        <Slide
          notes={[
            "We can also use the chair on the server side",
            "Either more traditionally loading the current state once",
            "and returning that from an API endpoint",
          ]}
        >
          <div className="mx-auto w-[30vw] scale-[200%]">
            <div className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925">
              <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
                chairServer.ts
              </span>
              <pre className="whitespace-pre-wrap break-words p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:break-words">
                <ChairLoadCode />
              </pre>
            </div>
          </div>
        </Slide>
        <Slide
          notes={[
            "Or starting a subscription and running a side effect",
            "like sending an email every time the chair changes",
          ]}
        >
          <div className="mx-auto w-[30vw] scale-[200%]">
            <div className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925">
              <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
                chairSubscription.ts
              </span>
              <pre className="whitespace-pre-wrap break-words p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:break-words">
                <ChairSubscriptionCode />
              </pre>
            </div>
          </div>
        </Slide>
        <Slide notes={["So the way Alice and Bob can design a chair together"]}>
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            <span className={userColors["alice"]}>Alice</span> and{" "}
            <span className={userColors["bob"]}>Bob</span> are <br />
            designing a chair.
          </h1>
        </Slide>
        <Slide notes={["Is by editing what's called a CoMap in Jazz"]}>
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            <span className={userColors["alice"]}>Alice</span> and{" "}
            <span className={userColors["bob"]}>Bob</span> are editing <br />a
            shared <span className="font-mono text-blue-500">CoMap</span>
          </h1>
        </Slide>
        <Slide
          notes={[
            "CoMap stands for Collaborative Map",
            "And it's the simplest and most common type of CoValue in Jazz",
            "Collaborative Values or CoValues",
            "is our general marketing term for CRDTs,",
            "because it sounds less autistic",
            "but they are CRDTs, so lets find out how they work in Jazz",
          ]}
        >
          <h1 className="text-center font-display text-6xl font-semibold tracking-tight">
            <span className={userColors["alice"]}>Alice</span> and{" "}
            <span className={userColors["bob"]}>Bob</span> are editing <br />a
            shared{" "}
            <span className="font-mono text-blue-500">Collaborative Map</span>
          </h1>
        </Slide>
        <Slide
          notes={[
            "CoMaps, like all other CoValues consist of three parts",
            "An immutable header",
            "It's main part: the append-only history",
            "And then the current state, which is derived from the history",
            "Initially the history is empty",
          ]}
        >
          <EffectiveTransactionsSlide
            scenario={scenario1}
            timestampIdx={0}
            showCore={true}
            showEditor={true}
          />
        </Slide>
        <Slide
          notes={["Now lets see what happens when Alice creates the chair"]}
        >
          <EffectiveTransactionsSlide
            scenario={scenario1}
            timestampIdx={1}
            showCore={true}
            showEditor={true}
          />
        </Slide>
        <Slide notes={["..."]}>
          <EffectiveTransactionsSlide
            scenario={scenario1}
            timestampIdx={2}
            showCore={true}
            showEditor={true}
          />
        </Slide>
        <Slide notes={["And then Bob and Alice keep editing it together"]}>
          <EffectiveTransactionsSlide
            scenario={scenario1}
            timestampIdx={3}
            showCore={true}
            showEditor={true}
          />
        </Slide>
        <Slide notes={["..."]}>
          <EffectiveTransactionsSlide
            scenario={scenario1}
            timestampIdx={4}
            showCore={true}
            showEditor={true}
          />
        </Slide>
        <Slide notes={["Bob is even using two different devices"]}>
          <EffectiveTransactionsSlide
            scenario={scenario1}
            timestampIdx={5}
            showCore={true}
            showEditor={true}
          />
        </Slide>
        <Slide notes={["..."]}>
          <EffectiveTransactionsSlide
            scenario={scenario1}
            timestampIdx={6}
            showCore={true}
            showEditor={true}
          />
        </Slide>
        <Slide
          notes={[
            "And now they're done",
            "And you can see how we can simply use the edit timestamps",
            "to decide what the current state should be",
            "this simple conflict resolution strategy is called last writer wins",
            "and it's actually what you want 99% of the time",
          ]}
        >
          <EffectiveTransactionsSlide
            scenario={scenario1}
            timestampIdx={7}
            showCore={true}
            showEditor={true}
          />
        </Slide>

        <Slide
          notes={[
            "So now that we've seen the internal structure of CoValues",
            "Let's see what syncing looks like in a distributed setting",
            "The basic idea is that similarly to Git,",
            "we only need to exchange diffs",
            "until everyone has the same full history",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for JSON”
          </h2>
        </Slide>

        <Slide
          notes={[
            "Let's say we've got the following setup",
            "With Alice having one device",
            "Bob having two",
            "And everyone connected to a sync and storage server",
          ]}
        >
          <EvenSimplerNewSyncDiagram />
        </Slide>

        <Slide
          notes={[
            "Now we'll run through the same sequence of edits",
            "but with each device showing its local version of the CoValue",
          ]}
        >
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={0}
            bob1Connection={1}
          />
        </Slide>
        <Slide notes={["..."]}>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={1}
            bob1Connection={1}
          />
        </Slide>
        <Slide
          notes={[
            "so far everyone immediately syncs and sees all changes",
            "so they also see the same derived state",
          ]}
        >
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={2}
            bob1Connection={1}
          />
        </Slide>
        <Slide notes={["Now let's say that Bob's browser device goes offline"]}>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={2}
            bob1Connection={"offline"}
          />
        </Slide>
        <Slide notes={["Bob can still make edits to his local state"]}>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={3}
            bob1Connection={"offline"}
          />
        </Slide>
        <Slide notes={["But no one else can see them"]}>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={4}
            bob1Connection={"offline"}
          />
        </Slide>
        <Slide
          notes={["And this device can't see changes that happen elsewhere"]}
        >
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={5}
            bob1Connection={"offline"}
          />
        </Slide>
        <Slide notes={["..."]}>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={6}
            bob1Connection={"offline"}
          />
        </Slide>
        <Slide notes={["So the states have now completely diverged"]}>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={7}
            bob1Connection={"offline"}
          />
        </Slide>
        <Slide notes={["But when this device reconnects"]}>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={7}
            bob1Connection={1}
          />
        </Slide>
        <Slide notes={["It quickly catches up on the changes it missed"]}>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={7}
            bob1Connection={2}
          />
        </Slide>
        <Slide
          notes={[
            "And in turn syncs its local changes that no one else has yet",
          ]}
        >
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={7}
            bob1Connection={3}
          />
        </Slide>
        <Slide notes={["..."]}>
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={7}
            bob1Connection={4}
          />
        </Slide>
        <Slide
          notes={[
            "And once everyone has the full history",
            "They also have the same derived state.",
            "So it turns out that data in a distributed system is hard",
            "but once you put the distributed system *into* the data",
            "everything becomes easy",
          ]}
        >
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={7}
            bob1Connection={5}
          />
        </Slide>

        <Slide
          notes={[
            "Now I promised you Git for JSON",
            "But I've only shown you a single simple map",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRDTs
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for JSON”
          </h2>
        </Slide>

        <Slide
          notes={["First you need to know that there are more CoValue types"]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CoMap
            <br />
            CoList
            <br />
            CoPlainText
            <br />
            CoRichText
            <br />
            CoVector
            <br />
            CoFileStream
            <br />
          </h1>
        </Slide>

        <Slide
          notes={[
            "And that CoValues can store references to other CoValues",
            "So they basically form a giant JSON tree, or graph",
            "And Jazz granularly lets you sync and subscribe to",
            "just the sub-trees of that graph that you need for certain parts of your app",
            "making it look like it's just nested plain old data",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CoValues in <JazzLogo className="inline-block h-32" />
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for infinite JSON graphs”
          </h2>
        </Slide>

        <Slide
          notes={[
            "You can actually also store blobs and streams in Jazz",
            "And reference those directly",
            "So you don't even need S3 or similar systems",
            "And file uploads become dead simple",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CoValues in <JazzLogo className="inline-block h-32" />
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for infinite JSON graphs + blobs”
          </h2>
        </Slide>

        <Slide
          notes={[
            "So this gives us extremely useful shared state out of the box",
            "And we didn't even need to build a backend stack",
            "The sync server is completely oblivious to your app, it just syncs diffs",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Shared State 😌
          </h1>
        </Slide>

        <Slide
          notes={[
            "And I'm sure its clear how you even get git-like branching flows",
            "that you can build into your app",
            "just from the way the history works",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Branching
          </h1>
        </Slide>

        <Slide notes={["..."]}>
          <img src={fork.src} className="mx-auto w-[70%] rounded-2xl" />
        </Slide>

        <Slide
          notes={[
            "Now where it gets really interesting",
            "Is that we found a way to decentralize permissions",
            "by using crypto...",
            "don't worry no blockchain stuff",
            "just good old signatures and encryption",
            "which guess what, you can do locally",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Permissions
          </h1>
        </Slide>

        <Slide notes={["Because if Alice simply..."]}>
          <HashAndSignatureSlide progressIdx={0} />
        </Slide>
        <Slide notes={["Calculates a rolling hash"]}>
          <HashAndSignatureSlide progressIdx={1} />
        </Slide>
        <Slide notes={["Over her edits"]}>
          <HashAndSignatureSlide progressIdx={2} />
        </Slide>
        <Slide notes={["..."]}>
          <HashAndSignatureSlide progressIdx={3} />
        </Slide>
        <Slide notes={["..."]}>
          <HashAndSignatureSlide progressIdx={4} />
        </Slide>
        <Slide
          notes={[
            "and keeps signing the latest hash",
            "everyone can independently verify",
            "that these are definitely her edits",
          ]}
        >
          <HashAndSignatureSlide progressIdx={5} />
        </Slide>
        <Slide notes={["And if bob does the same"]}>
          <HashAndSignatureSlide progressIdx={6} />
        </Slide>
        <Slide notes={["..."]}>
          <HashAndSignatureSlide progressIdx={7} />
        </Slide>
        <Slide notes={["..."]}>
          <HashAndSignatureSlide progressIdx={8} />
        </Slide>
        <Slide notes={["..."]}>
          <HashAndSignatureSlide progressIdx={9} />
        </Slide>
        <Slide notes={["..."]}>
          <HashAndSignatureSlide progressIdx={10} />
        </Slide>
        <Slide notes={["..."]}>
          <HashAndSignatureSlide progressIdx={11} />
        </Slide>
        <Slide notes={["..."]}>
          <HashAndSignatureSlide progressIdx={12} />
        </Slide>

        <Slide
          notes={[
            "And we keep track somewhere of who is allowed to write to that CoValue",
            "Then we have effectively implemented write permissions",
          ]}
        >
          <HashAndSignatureSlide progressIdx={13} />
        </Slide>
        <Slide
          notes={[
            "where do we keep track? Well in another CoValue of course",
            "Which we call a Group, which is basically a role-based access control list",
            "which you can create locally and sync just like CoValues themselves",
          ]}
        >
          <HashAndSignatureSlide progressIdx={13} highlightGroup={true} />
        </Slide>
        <Slide notes={["Now all we need for read access..."]}>
          <HashAndSignatureSlide progressIdx={13} />
        </Slide>
        <Slide
          notes={[
            "is to encrypt our history entries",
            "And only give the encryption key to people we want to have read-access",
          ]}
        >
          <div className="mx-auto">
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
        </Slide>
        <Slide
          notes={[
            "Where do we put the key? Well of course also into the Group!",
          ]}
        >
          <div className="mx-auto">
            <CoValueCoreDiagram
              header={scenario1.header}
              sessions={scenario1.sessions}
              showView={true}
              showCore={true}
              showHashAndSignature={true}
              hashProgressIdx={13}
              encryptedItems={true}
              highlightOwner={true}
            />
          </div>
        </Slide>
        <Slide
          notes={[
            "And remember how we said the sync server is oblivious, it only needs to sync diffs",
          ]}
        >
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={6}
            bob1Connection={5}
            serverEncrypted={false}
          />
        </Slide>
        <Slide
          notes={[
            "Well it doesn't even need to see what's in the diffs, it can just sync the encrypted diffs",
            "And we don't have to give it read access",
            "So if we run the sync server (and we do that well at affordable prices!)",
            "Your app's users don't have to trust us with their data",
            "And in the most extreme case where your users manage their own keys",
            "You can trivially build end-to-end-encrypted apps",
          ]}
        >
          <CoValueSyncDiagram
            scenario={scenario1}
            timestampIdx={6}
            bob1Connection={5}
            serverEncrypted={true}
          />
        </Slide>

        <Slide
          notes={["So, to summarize so far", "We get shared state, blobs"]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CoValues in <JazzLogo className="inline-block h-32" />
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for infinite JSON graphs + blobs”
          </h2>
        </Slide>
        <Slide notes={["and strong permissions", "all from one abstraction"]}>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CoValues in <JazzLogo className="inline-block h-32" />
          </h1>
          <h2 className="text-center font-display text-6xl font-semibold">
            “Git for infinite JSON graphs + blobs
            <br />
            with permissions”
          </h2>
        </Slide>

        <Slide
          notes={[
            "Or said another way,",
            "Jazz commoditizes most backend, database",
            "and data fetching concerns",
            "and decentralizes the whole thing",
            "which is great news",
          ]}
        >
          <img src={crud.src} className="mx-auto w-[60%] rounded-2xl" />
        </Slide>

        <Slide
          notes={[
            "And even better, we're building everything you need around that, too",
          ]}
        >
          <FeaturesSectionSparse />
        </Slide>

        <Slide
          notes={[
            "All of that is what we've open-sourced,",
            "launched and given to people at Local-First Conf 2024",
            "And what they did with it ",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Local-first conf 2024 .. now
          </h1>
        </Slide>

        <Slide notes={["surprised us in many ways"]}>
          <img
            src={monkeyAk47Img.src}
            alt="Monkey Ak47"
            className="mx-auto w-[70%] rounded-2xl"
          />
        </Slide>

        <Slide
          notes={[
            "People are building and launching all kinds of stuff with Jazz",
            "Including things like Figma and Notion alternatives",
            "which makes me really happy because that was exactly the point",
            "but also really wild things",
            "like social networks",
            "dystopian tele-robotics startups",
          ]}
        >
          <div className="flex flex-wrap items-baseline justify-center gap-8 px-[10%]">
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Read.cv alternative
            </h1>
            <h1 className="font-display text-6xl font-semibold tracking-tight">
              Construction Auditing App
            </h1>
            <h1 className="font-display text-3xl font-semibold tracking-tight">
              Inspiration Canvas App
            </h1>
            <h1 className="font-display text-8xl font-semibold tracking-tight">
              Figma Alternative
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Science-based Fitness App
            </h1>
            <h1 className="font-display text-5xl font-semibold tracking-tight">
              CRM for Friends
            </h1>
            <h1 className="font-display text-6xl font-semibold tracking-tight">
              Are.na alternative
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Photo Journal App
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Dream Tracking App
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Trip Planning App
            </h1>
            <h1 className="font-display text-8xl font-semibold tracking-tight">
              Notion Alternative
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              LLM Language Learning App
            </h1>
            <h1 className="font-display text-5xl font-semibold tracking-tight">
              Event Planning App
            </h1>
            <h1 className="font-display text-6xl font-semibold tracking-tight">
              Discord/Slack Alternative (x2)
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Social Network
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Dystopian Tele-robotics Startup
            </h1>
            <h1 className="font-display text-3xl font-semibold tracking-tight">
              Personal Finance App
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Resumable Streams Abstraction
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Invoice Tracking App
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Point-of-service App
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Couple's Life Management App
            </h1>
          </div>
        </Slide>

        <Slide
          notes={[
            "and most surprisingly",
            "extremely normal apps",
            "that use Jazz both in the frontend and in the backend",
            "and even pure backend use cases...",
            "that just use Jazz like a normal database...?",
          ]}
        >
          <div className="flex flex-wrap items-baseline justify-center gap-8 px-[10%]">
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Read.cv alternative
            </h1>
            <h1 className="font-display text-6xl font-semibold tracking-tight">
              Construction Auditing App
            </h1>
            <h1 className="font-display text-3xl font-semibold tracking-tight">
              Inspiration Canvas App
            </h1>
            <h1 className="font-display text-8xl font-semibold tracking-tight">
              Figma Alternative
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Science-based Fitness App
            </h1>
            <h1 className="font-display text-5xl font-semibold tracking-tight">
              CRM for Friends
            </h1>
            <h1 className="font-display text-6xl font-semibold tracking-tight">
              Are.na alternative
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Photo Journal App
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Dream Tracking App
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Trip Planning App
            </h1>
            <h1 className="font-display text-8xl font-semibold tracking-tight">
              Notion Alternative
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              LLM Language Learning App
            </h1>
            <h1 className="font-display text-5xl font-semibold tracking-tight">
              Event Planning App
            </h1>
            <h1 className="font-display text-6xl font-semibold tracking-tight">
              Discord/Slack Alternative (x2)
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Social Network
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Dystopian Tele-robotics Startup
            </h1>
            <h1 className="font-display text-3xl font-semibold tracking-tight">
              Personal Finance App
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Resumable Streams Abstraction
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Invoice Tracking App
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Point-of-service App
            </h1>
            <h1 className="font-display text-4xl font-semibold tracking-tight">
              Couple's Life Management App
            </h1>
          </div>
        </Slide>

        <Slide notes={["Turns out we're building a database!"]}>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            <span className="text-[2.35em] font-extralight">PART 2</span>
            <br />
            Turns out
            <br />
            we're building
            <br />a database
          </h1>
        </Slide>

        <Slide
          notes={[
            "Initially this felt a little awkward to say",
            "But we realized it makes a lot more sense to people",
            "Especially if they've never heard of sync engines and local-first",
            "And this new framing has been validated in a bunch of ways now",
          ]}
        >
          <img src={ken.src} className="mx-auto w-[40%] rounded-2xl" />
        </Slide>

        <Slide
          notes={[
            "Most recently, we were able to do a collab with Vercel",
            "to end up being the first external open-source backend that",
            "you can use their new Workflow Development Kit with.",
            "Workflows are super interesting and you should check them out",
          ]}
        >
          <img src={vercel.src} className="mx-auto w-[60%] rounded-2xl" />
        </Slide>

        <Slide
          notes={[
            "Because they're an even higher abstraction than sync engines",
            "Letting you express multi-step backend tasks using simple code",
            "with intermediate results being transparently stored",
            "and steps being retried until they succeed.",
          ]}
        >
          <img src={vercel.src} className="mx-auto w-[60%] rounded-2xl" />
        </Slide>

        <Slide
          notes={[
            "Let's talk a bit more about compute, storage and the cloud",
            "Because as cool as CRDTs are on the client",
            "What I find really interesting is",
            "what they let you do with your architecture",
            "Remember this is roughly what our sync & storage infra looks like",
          ]}
        >
          <SimpleNewSyncDiagram />
        </Slide>

        <Slide
          notes={[
            "Now contrast this with a traditional stack",
            "Which is not realtime",
            "is only fast in one region",
            "has single point of failure",
            "and definitely doesn't work offline",
          ]}
        >
          <TradDBDiagram />
        </Slide>

        <Slide
          notes={[
            "Sync engines give you a real-time read path",
            "that works beautifully with reactive UIs, and agents in the backend",
            "but writes still have to go to a central point",
            "and if you're lucky you get optimistic local updates",
          ]}
        >
          <SyncEngineDiagram />
        </Slide>

        <Slide
          notes={[
            "Finally, maybe the most popular choice today are Durable Objects",
            "our friends at CloudFlare have cooked something interesting here",
            "but I think unfortuantely it's still kinda basic and we need higher level abstractions",
          ]}
        >
          <DurableObjectsDiagram />
        </Slide>

        <Slide
          notes={[
            "Durable objects are basically tiny serverless servers",
            "that run as close to their connected clients as they can",
          ]}
        >
          <DurableObjectsDiagram />
        </Slide>

        <Slide notes={["And intelligently get moved around"]}>
          <DurableObjectsDiagram2 />
        </Slide>

        <Slide
          notes={[
            "Based on where requests are coming from",
            "But they still require you to handle networking and duplicate state management",
            "They don't work offline",
            "And most annoyingly, they kind of force you to chunk up your data",
            "into things that work as independent rooms with bundled compute and storage",
            "each of which are tiny but not really tiny enough",
          ]}
        >
          <DurableObjectsDiagram3 />
        </Slide>

        <Slide
          notes={[
            "comparing this to our infra again",
            "because the sync server is oblivious to your app",
            "we can have lots of sync servers everywhere that sync tiny bits of data for many different apps",
            "while most of the compute goes away because state sharing and permissions are standardized by CoValues",
          ]}
        >
          <SimpleNewSyncDiagram />
        </Slide>

        <Slide
          notes={[
            "for the little bits of compute that you still need",
            "like external side effects, integrations and complex business logic",
            "you can use any kind of lightweight serverless compute you want",
            "which, just like the client syncs just the data it needs",
          ]}
        >
          <SimpleNewSyncDiagramWithLambda />
        </Slide>

        <Slide notes={[
          "And you can do even cooler stuff",
          "Like having RPC calls to your server worker that syncs data alongside the request",
          "so you don't need to redundantly fetch it from the sync server",
          "note that we can only do this because everyone can independently verify",
          "the authenticity of the synced edits"
        ]}>
          <SimpleNewSyncDiagramWithLambdaAndRPC />
        </Slide>

        <Slide notes={[
          "and even funkier",
          "your server worker can do server side rendering",
          "for really fast initial loads",
          "but then send CRDT diffs alongside the HTML to hydrate both data and UI",
          "giving you the holy grail of fast statically loaded pages",
          "that become rich interactive realtime, local-first apps",
          "because..."
        ]}>
          <SimpleNewSyncDiagramWithLambdaAndSSR />
        </Slide>

        <Slide notes={["You can just sync things."]}>
          <img
            src={justSync.src}
            className="mx-auto w-[40%] -rotate-6 rounded-2xl"
          />
        </Slide>

        <Slide notes={["We're really doubling down on our infrastructure right now"]}>
          <SimpleNewSyncDiagram />
        </Slide>

        <Slide notes={["Both to reach basically infinite scale across all Jazz apps",
          "And to make maximum use of local-first properties for availability and low-latency",
          "using ideas from both databases like storage-layer sharding",
          "and from CDNs, like IP Anycast for local-routing and super fast failover"
        ]}>
          <NewSyncDiagram />
        </Slide>

        <Slide
          notes={[
            "Weird situation: can do things DBs, sync engines and other solutions can't, and people are successfully building with it. but are missing DB table stakes",
          ]}
        >
          <img src={venn.src} className="mx-auto w-[70%] rounded-2xl" />
        </Slide>

        <Slide notes={["The most requested feature right now is supporting indices and complex queries",
          "It's kind of funny just how much data Jazz can cram onto a client",
          "that you can brute force filter, sort and paginate over",
          "but for large datasets you need indices",
          "so we're inventing how that fits into our CRDT and distributed infra world"
        ]}>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Indices & Complex Queries
          </h1>
        </Slide>

        <Slide notes={["one obvious solution for most apps will be",
          "that you have a dedicated index worker, that stores an index in another CoValue",
          "which is actually small enough to fit on clients",
          "which can then directly execute queries and fetch entries super quickly"
        ]}>
          <SimpleNewSyncDiagramIndexWorker />
        </Slide>

        <Slide notes={["or even more wirdly", "if the indices are covalues, they can be maintained",
          "in a collaborative way where beefier clients", "share the workload of creating the index",
          "while weaker devices can benefit from the index",
          "all without a server part at all!"
        ]}>
          <SimpleNewSyncDiagramIndexClient />
        </Slide>

        <Slide notes={["Now one giant thing that I've glanced over this whole time",
          "is that a lot of things are easy for us because we've made a pact with the devil",
          "by going all in on eventual consistency",
          "and in our experience a crazy amount of state across apps",
          "is actually totally ok to be eventually consistent"]}>
          <img src={tradeOffer.src} className="mx-auto w-[40%] rounded-2xl" />
        </Slide>

        <Slide notes={["But people keep wanting to use Jazz for everything",
          "including state where you really want global consistency",
          "like purchase flows, bookings, etc.",
          "So we're also figuring out how to make that happen in a Jazz way"
        ]}>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Strongly Consistent Transactions
          </h1>
        </Slide>

        <Slide notes={["Let's have a quick look again how other architectures",
          "handle strong consistency.",
          "Traditional databases of course just have a single write replica"
        ]}>
          <TradDBDiagram />
        </Slide>

        <Slide notes={["Which becomes the transaction authority"]}>
          <TradDBDiagram authority={true} />
        </Slide>

        <Slide notes={["Sync engines are pretty much the same"]}>
          <SyncEngineDiagram />
        </Slide>

        <Slide notes={["With the database still having that role"]}>
          <SyncEngineDiagram authority={true} />
        </Slide>

        <Slide notes={["Durable object are similar in that each object is its own transaction authority",
          "which is much nicer and much more fine-grained",
          "but still requires you to architect your app to fit into that model",
          "and still forces strong consistency on everything"]}>
          <DurableObjectsDiagram3 />
        </Slide>

        <Slide notes={["So you always pay the latency and availability cost of that"]}>
          <DurableObjectsDiagram3 authority={true} />
        </Slide>

        <Slide notes={["With jazz, the simplest thing you can do right now"]}>
          <SimpleNewSyncDiagram />
        </Slide>

        <Slide notes={["Is to have a single worker that has exclusive write access",
          "to a bit of data which is then still synced for reading the normal way",
          "basically giving you a tiny sync engine with strong consistency",
          "just for that bit of data",
          "but this doesn't scale beyond one worker"
        ]}>
          <SimpleNewSyncDiagramWithLambda authority={true} />
        </Slide>

        <Slide notes={["So in addition, we're working on a way",
          "to make edge servers in our infra a dynamic transaction authority",
        ]}>
          <SimpleNewSyncDiagram />
        </Slide>

        <Slide notes={["where just the transaction approving part can move around",
          "like a durable object",
          ]}>
          <SimpleNewSyncDiagram authority={1} />
        </Slide>

        <Slide notes={[
          "direcly from connected clients",
          "and the availability and latency tradeoff",
          "being super granular to just that bit of data"]}>
          <SimpleNewSyncDiagram authority={2} />
        </Slide>

        <Slide notes={["Finally I want to talk about the bigger picture a little bit"]}>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            <span className="text-[2.35em] font-extralight">PART 3</span>
            <br />
            Ready for the future
          </h1>
        </Slide>

        <Slide
          notes={[
            "a natural reaction to all this is",
            "why go to so much effort to implement offline-first and cryptographic permissions",
            "devices hardly go offline anymore",
            "and while I would argue that we still encounter flaky connections every day",
            "I simply want you to think bigger"
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Why care about local-first?
          </h1>
        </Slide>

        <Slide notes={[
          "no, even bigger"
          ]}>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            PlanetScale
          </h1>
        </Slide>

        <Slide notes={[
          "We're going to be multi-planetary,",
          "Whether it takes Elon or another lunatic"
        ]}>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            <span className="line-through">PlanetScale</span> Solar System
            Scale.
          </h1>
        </Slide>

        <Slide
          notes={[
            "And even in the meantime",
            "when us-east-1 is acting up it might as well be on mars, latency wise",
            "oh we're moving to AWS btw",
            "but we're not afraid of high latencies"
          ]}
        >
          <img src={mars.src} className="mx-auto w-[40%] rounded-2xl" />
        </Slide>

        <Slide notes={["because Jazz is a space-first database"]}>
          <img
            src={space.src}
            className="mx-auto w-[40%] rotate-6 rounded-2xl"
          />
        </Slide>

        <Slide notes={["and for some it might be the end of the world when us-east-1 goes down completely",
          "but Jazz only half jokingly was built for the apocalypse as well"
        ]}>
          <img
            src={apocalypse.src}
            className="mx-auto w-[40%] rotate-3 rounded-2xl"
          />
        </Slide>

        <Slide notes={["If this all sounded cool, come hang out!"]}>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            The End: Come Hang Out!
          </h1>
        </Slide>

        <Slide notes={["If you haven't yet, you can check out the apocalypse-first",
          "properties of jazz youself",
          "by disconnecting lan cables on a running Jazz app"
        ]}>
          <img src={booth.src} className="mx-auto w-[40%] rounded-2xl" />
        </Slide>

        <Slide notes={["And we can't wait to see what you'll build when you try Jazz",
          "because what always makes us happiest is not when we get praise",
          "but when apps that were built with Jazz get praise"
        ]}>
          <img src={praise.src} className="mx-auto w-[40%] rounded-2xl" />
        </Slide>

        <Slide notes={["Jazz is starting to power some serious stuff",
          "but it was always built with side projects in mind",
          "and consistently the first thing people say when they see Jazz is",
          "oh, I can finally build this idea I had now"
        ]}>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            We ❤️ side projects
          </h1>
        </Slide>

        <Slide notes={["Which is why we're hosting a side-project party tomorrow",
          "And you're all invited!",
          "Well we can probably fit another 30 people or so",
          "Drop by after the community day",
          "Hang out, try jazz or just build something"
        ]}>
          <img src={spp.src} className="mx-auto w-[60%] rounded-2xl" />
        </Slide>

        <Slide notes={["Thank you very much"
        ]}>
          <img src={sync.src} className="mx-auto w-[60%] rounded-2xl" />
        </Slide>
      </Slides>
    </div>
  );
}
