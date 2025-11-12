import { JazzLogo } from "@/components/forMdx";
import { IntroSlide } from "../shared/slides/IntroSlide";
import { Slide, Slides } from "@/components/Slides";
import { SimpleCentered } from "../shared/slides/Containers";
import {
  EvenSimplerNewSyncDiagram,
  NewSyncDiagram,
  SimpleNewSyncDiagram,
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
import { JazzSyncs } from "@/components/icons/JazzSyncs";

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
            <span className="text-[2.35em] font-extralight">PART 2</span>
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
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Ink & Switch. TODO: pic
          </h1>
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
            1) What if every app could be real-time multiplayer?
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
            Shared State check
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
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            You wouldn't fork a JSON
          </h1>
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
            "where do we keep track? Well in another CoValue of course",
            "Which we call a Group, which is basically a role-based access control list",
            "which you can create locally and sync just like CoValues themselves",
          ]}
        >
          <HashAndSignatureSlide progressIdx={13} />
        </Slide>
        <Slide notes={["Now all we need for read access..."]}>
          <HashAndSignatureSlide progressIdx={13} />
        </Slide>
        <Slide
          notes={[
            "is to encrypt our history entries",
            "And only give the encryption key to people we want to have read-access",
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
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            CRUD meme
          </h1>
        </Slide>

        <Slide
          notes={[
            "And even better, we're building everything you need around that, too",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Batteries included
          </h1>
        </Slide>

        <Slide
          notes={[
            "All of that is what we've open-sourced,",
            "launched and given to people at Local-first conf 2024",
            "And what they did with it ",
          ]}
        >
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Local-first conf 2024
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
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            What people have been building
          </h1>
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
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            What people have been building
          </h1>
        </Slide>

        <Slide notes={[""]}>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            <span className="text-[2.35em] font-extralight">PART 2</span>
            <br />
            How we found out
            <br />
            we're building
            <br />a database
          </h1>
        </Slide>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Vercel pic
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <SimpleNewSyncDiagram />
        </SimpleCentered>

        <SimpleCentered>
          <NewSyncDiagram />
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            vs trad db
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            vs sync engines
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            vs durable objects
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Durable Objects ain't it.
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Don't need to isolate compute per tenant/object
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Don't need to bundle compute & storage
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            SSR
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Just sync it
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Weird situation: can do things DBs, sync engines and other solutions
            can't, and people are successfully building with it. but are missing DB table stakes
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Indices & Complex Queries
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Diagram: Indices built by worker
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Diagram: Indices built collaboratively
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Trade offer: eventual consistency
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Strongly Consistent Transactions
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Diagrams again, focus on transactionality
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            PART IV: Ready for the future
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            why go to so much effort to implement offline-first and local state
            & permissions?
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            the thing is: when I first saw CRDTs and crypto permissions
            together, I realised we can build for even more extreme scenarios
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            <span className="line-through">PlanetScale</span> Solar System
            Scale.
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            and if us-east-1 is down the world might as well be ending
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Apocalypse-first.
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            The End: Come Hang Out!
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            (Picture of Booth)
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Story about side projects
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            Apps using jazz get praised meme
          </h1>
        </SimpleCentered>

        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            SPP
          </h1>
        </SimpleCentered>
      </Slides>
    </div>
  );
}
