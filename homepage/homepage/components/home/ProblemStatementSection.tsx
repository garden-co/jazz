import { DiagramAfterJazz } from "@/components/DiagramAfterJazz";
import { DiagramBeforeJazz } from "@/components/DiagramBeforeJazz";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";
import { SectionHeader } from "@garden-co/design-system/src/components/molecules/SectionHeader";

export default function ProblemStatementSection() {
  return (
    <>
      <SectionHeader
        className="sm:mx-auto sm:text-center"
        title={"A database that does what's actually needed"}
        slogan="A perspective shift worth 10,000 hours"
      />

      <div className="grid overflow-hidden rounded-lg border shadow-xs sm:grid-cols-2 md:rounded-xl">
        <div className="relative flex flex-col border-b bg-stone-50 p-4 pb-8 dark:bg-transparent sm:border-b-0 sm:border-r md:p-8">
          <span className="absolute -right-4 top-0 hidden h-full items-center sm:flex md:-right-6">
            <span className="rounded-full bg-stone-200 p-1 dark:bg-stone-900 dark:text-white md:p-3">
              <Icon name="arrowRight" />
            </span>
          </span>
          <span className="absolute -bottom-6 left-0 flex w-full justify-center sm:hidden">
            <span className="rounded-full bg-stone-200 p-3 dark:bg-stone-900 dark:text-white">
              <Icon name="arrowDown" />
            </span>
          </span>
          <Prose>
            <p className="font-display text-lg font-semibold text-highlight md:text-xl">
              Every stack is a re-invention of shared state.
            </p>
          </Prose>
          <div className="relative flex flex-1 items-center">
            <div className="pointer-events-none absolute left-0 top-0 z-10 h-full w-20 bg-linear-to-r from-stone-50 from-25% to-transparent dark:from-stone-950"></div>
            <div className="pointer-events-none absolute left-0 top-0 z-10 h-20 w-full bg-linear-to-b from-stone-50 from-25% to-transparent dark:from-stone-950"></div>
            <div className="pointer-events-none absolute bottom-0 left-0 z-10 h-20 w-full bg-linear-to-t from-stone-50 from-25% to-transparent dark:from-stone-950"></div>
            <div className="pointer-events-none absolute right-0 top-0 z-10 h-full w-20 bg-linear-to-l from-stone-50 from-25% to-transparent dark:from-stone-950"></div>

            <DiagramBeforeJazz className="max-w-full" />
          </div>
          <Prose>
            <p>
              For each new app you tackle a{" "}
              <strong>mess of moving parts and infra worries.</strong> Your
              backend is responsible for shuffling data around in a myriad of
              ways.
            </p>
            <p>
              Want to build a <strong>modern app</strong> with multiplayer or
              offline-support? <strong>Figma, Notion and Linear</strong> all had
              to spend <strong>years</strong> on completely custom stacks.
            </p>
          </Prose>
        </div>
        <div className="flex flex-col gap-3 p-4 pt-8 md:gap-5 md:p-8">
          <Prose>
            <p className="font-display text-lg font-semibold text-highlight md:text-xl">
              What if we started from shared state?
            </p>
          </Prose>
          <div className="flex flex-1 items-center">
            <DiagramAfterJazz className="mx-auto h-auto w-full max-w-sm" />
          </div>
          <Prose>
            <p>
              Jazz gives you <strong>local state</strong> that’s{" "}
              <strong>instantly synced and stored in the cloud.</strong>{" "}
              Including images and files.{" "}
              <strong>With users &amp; permissions built-in.</strong>
            </p>
            <p>
              With a <strong>DB and infra made for modern apps</strong> you get
              to focus on <strong>building the app your users want.</strong>{" "}
              You'll notice that <strong>90% of the work is now the UI.</strong>
            </p>
          </Prose>
        </div>
      </div>
    </>
  );
}
