import { Prose } from "@/components/molecules/Prose";

export function Typography() {
  return (
    <>
      <h2 id="typography" className="text-xl mt-5 mb-2 font-bold">
        Typography
      </h2>

      <div className="grid gap-4">
        <div>
          Heading 1
          <Prose className="p-3">
            <h1>Ship top-tier apps at high tempo</h1>
          </Prose>
        </div>
        <div>
          Heading 2
          <Prose className="p-3">
            <h2>Ship top-tier apps at high tempo</h2>
          </Prose>
        </div>
        <div>
          Heading 3
          <Prose className="p-3">
            <h3>Ship top-tier apps at high tempo</h3>
          </Prose>
        </div>
        <div>
          Heading 4
          <Prose className="p-3">
            <h4>Ship top-tier apps at high tempo</h4>
          </Prose>
        </div>
        <div>
          Paragraph
          <p className="text-xs text-highlight my-1">
            NB: That text can be styled with colour classes, including{" "}
            <code>text-muted</code> and <code>text-highlight</code>, see{" "}
            <a href="#text-color-variables">Text Color Variables</a>.
          </p>
          <Prose className="p-3">
            <p>
              <strong>Jazz is a framework for building local-first apps</strong>{" "}
              — an architecture that lets companies like Figma and Linear play
              in a league of their own.
            </p>

            <p>
              Open source. Self-host or use Jazz Cloud for zero-config magic.
            </p>
          </Prose>
        </div>

        <div>
          Link
          <Prose className="p-3">
            This is a <a href="https://jazz.tools">link</a>
          </Prose>
        </div>

        <div>
          Code
          <Prose className="p-3">
            This is a one-line <code>piece of code</code>
          </Prose>
        </div>
      </div>
    </>
  );
}
