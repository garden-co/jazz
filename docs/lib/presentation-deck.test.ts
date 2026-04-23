import test from "node:test";
import assert from "node:assert/strict";
import {
  estimatePresentationSpeakingDurationSeconds,
  parsePresentationSlidesFromMdx,
  readLetterCanvasArrowNavigationDirection,
  resolvePresentationSlideIdentity,
} from "./presentation-deck.ts";

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
      estimatedDurationSeconds: 4,
      href: "/presentations/react-miami#slide=intro",
      notesText: "Welcome everyone.",
      notesHref: "/presenter/react-miami#slide=intro",
      slug: "intro",
      title: "Jazz at React Miami",
    },
    {
      estimatedDurationSeconds: 0,
      href: "/presentations/react-miami#slide=why-jazz",
      notesText: "",
      notesHref: "/presenter/react-miami#slide=why-jazz",
      slug: "why-jazz",
      title: "Why Jazz?",
    },
  ]);
});

test("defaults missing slug and title to the 1-based slide index", () => {
  const slides = parsePresentationSlidesFromMdx(
    "react-miami",
    `
<Slide>
  <h1>Opening</h1>
</Slide>

<Slide title="Custom title">
  <h1>Second</h1>
</Slide>

<Slide slug="custom-slug">
  <h1>Third</h1>
</Slide>

<Slide slug="" title="">
  <h1>Fourth</h1>
</Slide>
`,
  );

  assert.deepEqual(slides, [
    {
      estimatedDurationSeconds: 0,
      href: "/presentations/react-miami#slide=1",
      notesText: "",
      notesHref: "/presenter/react-miami#slide=1",
      slug: "1",
      title: "1",
    },
    {
      estimatedDurationSeconds: 0,
      href: "/presentations/react-miami#slide=2",
      notesText: "",
      notesHref: "/presenter/react-miami#slide=2",
      slug: "2",
      title: "Custom title",
    },
    {
      estimatedDurationSeconds: 0,
      href: "/presentations/react-miami#slide=custom-slug",
      notesText: "",
      notesHref: "/presenter/react-miami#slide=custom-slug",
      slug: "custom-slug",
      title: "3",
    },
    {
      estimatedDurationSeconds: 0,
      href: "/presentations/react-miami#slide=4",
      notesText: "",
      notesHref: "/presenter/react-miami#slide=4",
      slug: "4",
      title: "4",
    },
  ]);
});

test("reuses parsed fallback identity for rendered slides with omitted props", () => {
  const slides = parsePresentationSlidesFromMdx(
    "react-miami",
    `
<Slide>
  <h1>Opening</h1>
</Slide>

<Slide title="Custom title">
  <h1>Second</h1>
</Slide>
`,
  );

  assert.deepEqual(resolvePresentationSlideIdentity(slides, 0, {}), {
    slug: "1",
    title: "1",
  });

  assert.deepEqual(resolvePresentationSlideIdentity(slides, 1, { title: "Custom title" }), {
    slug: "2",
    title: "Custom title",
  });
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

test("estimates speaking duration from note text length and structure", () => {
  assert.equal(estimatePresentationSpeakingDurationSeconds(""), 0);
  assert.equal(estimatePresentationSpeakingDurationSeconds("Welcome everyone."), 4);
  assert.equal(
    estimatePresentationSpeakingDurationSeconds(
      "This slide sets up the overall motivation for Jazz and frames the rest of the talk. We want to explain what changed, why now, and what people should listen for.",
    ),
    11,
  );
  assert.equal(
    estimatePresentationSpeakingDurationSeconds("First paragraph.\n\nSecond paragraph."),
    6,
  );
});

test("reads slide navigation from letter canvas arrow messages", () => {
  assert.equal(
    readLetterCanvasArrowNavigationDirection({
      key: "ArrowRight",
      type: "jazz-letter-canvas:arrow-key",
    }),
    "next",
  );
  assert.equal(
    readLetterCanvasArrowNavigationDirection({
      key: "ArrowLeft",
      type: "jazz-letter-canvas:arrow-key",
    }),
    "previous",
  );
  assert.equal(
    readLetterCanvasArrowNavigationDirection({
      key: "ArrowUp",
      type: "jazz-letter-canvas:arrow-key",
    }),
    null,
  );
  assert.equal(readLetterCanvasArrowNavigationDirection({ key: "ArrowRight" }), null);
});
