import { JazzLogo } from "@/components/forMdx";
import { IntroSlide } from "../shared/slides/IntroSlide";
import { Slides } from "@/components/Slides";
import { SimpleCentered } from "../shared/slides/Containers";
import { AnimatedSvgEdge } from "@/components/animated-svg-edge";
import { Background, ReactFlow } from "@xyflow/react";
import { NewSyncDiagram } from "./NewSyncDIagram";

export default function Page() {
  return (
    <div className="flex h-screen w-full flex-col items-center justify-start gap-5 bg-black p-5 text-white">
      <Slides>
        <IntroSlide
          talkTitle={
            <>
              Oops, my<br/>sync engine<br/>has become<br/>a database.
            </>
          }
          eventName="Sync Conf"
          eventDate="November '25"
        />


        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            2019: Figma.
          </h1>
        </SimpleCentered>
        <SimpleCentered>
          <h1 className="text-center font-display text-8xl font-semibold tracking-tight">
            2019: Notion.
          </h1>
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
            2020: CRDTs.
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
      </Slides>
    </div>
  );
}