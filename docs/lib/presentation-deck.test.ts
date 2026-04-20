import test from "node:test";
import assert from "node:assert/strict";
import { parsePresentationSlidesFromMdx } from "./presentation-deck.ts";

test("parses slide order from the MDX file in source order", () => {
  const slides = parsePresentationSlidesFromMdx(
    "react-miami",
    `
<Slide slug="intro" title="Jazz at React Miami">
  <h1>Jazz at React Miami</h1>
  <Notes>
    Welcome everyone.
  </Notes>
</Slide>

<Slide slug="why-jazz" title="Why Jazz?">
  <h1>Why Jazz?</h1>
</Slide>
`,
  );

  assert.deepEqual(slides, [
    {
      href: "/presentations/react-miami/intro",
      slug: "intro",
      title: "Jazz at React Miami",
    },
    {
      href: "/presentations/react-miami/why-jazz",
      slug: "why-jazz",
      title: "Why Jazz?",
    },
  ]);
});

test("rejects duplicate slide slugs inside one deck", () => {
  assert.throws(
    () =>
      parsePresentationSlidesFromMdx(
        "react-miami",
        `
<Slide slug="intro" title="Intro"></Slide>
<Slide slug="intro" title="Second intro"></Slide>
`,
      ),
    /duplicate slide slug "intro"/i,
  );
});

test("requires at least one exported slide", () => {
  assert.throws(
    () => parsePresentationSlidesFromMdx("react-miami", "# No slides here"),
    /must define at least one <Slide/i,
  );
});

test("requires slide titles directly on the Slide tag", () => {
  assert.throws(
    () =>
      parsePresentationSlidesFromMdx(
        "react-miami",
        `
<Slide slug="intro">
  <h1>Jazz at React Miami</h1>
</Slide>
`,
      ),
    /must include string "slug" and "title" attributes/i,
  );
});
