import { DiagramAfterJazz } from "@/components/DiagramAfterJazz";
import { DiagramBeforeJazz } from "@/components/DiagramBeforeJazz";
import { Icon } from "gcmp-design-system/src/app/components/atoms/Icon";
import { Prose } from "gcmp-design-system/src/app/components/molecules/Prose";
import { SectionHeader } from "gcmp-design-system/src/app/components/molecules/SectionHeader";

export default function ProblemStatementSection() {
  return (
    <div className="container grid gap-4 lg:gap-8">
      <SectionHeader
        className="sm:text-center sm:mx-auto"
        title={"Powered by the first “flat stack”"}
        slogan="A perspective shift worth 10,000 hours"
      />

      <div className="grid sm:grid-cols-2 border rounded-lg shadow-sm md:rounded-xl overflow-hidden dark:border-stone-900">
        <div className="flex flex-col bg-stone-50 relative gap-3 p-4 pb-8 md:p-8 md:gap-5 border-b sm:border-b-0 sm:border-r dark:bg-transparent dark:border-stone-900">
          <span className="hidden absolute top-0 -right-4 md:-right-6 sm:flex items-center h-full">
            <span className="p-1 md:p-3 bg-stone-200 rounded-full dark:bg-stone-900 dark:text-white">
              <Icon name="arrowRight" />
            </span>
          </span>
          <span className="sm:hidden w-full absolute -bottom-6 flex justify-center left-0">
            <span className="p-3 bg-stone-200 rounded-full dark:bg-stone-900 dark:text-white">
              <Icon name="arrowDown" />
            </span>
          </span>
          <Prose>
            <p className="font-display text-lg md:text-xl font-semibold text-stone-900 dark:text-white">
              Every stack is a re-invention of shared state.
            </p>
          </Prose>
          <div className="relative flex items-center flex-1">
            <div className="w-20 h-full bg-gradient-to-r from-stone-50 to-transparent absolute top-0 left-0 z-10 dark:from-stone-950"></div>
            <div className="h-20 w-full bg-gradient-to-b from-stone-50 to-transparent absolute top-0 left-0 z-10 dark:from-stone-950"></div>
            <div className="h-20 w-full bg-gradient-to-t from-stone-50 to-transparent absolute bottom-0 left-0 z-10 dark:from-stone-950"></div>
            <div className="w-20 h-full bg-gradient-to-l from-stone-50 to-transparent absolute top-0 right-0 z-10 dark:from-stone-950"></div>

            <DiagramBeforeJazz className="mx-auto w-full h-auto max-w-sm" />
          </div>
          <Prose>
            <p>
              For each new app you tackle a{" "}
              <strong>mess of moving parts and infra worries.</strong> Or, you
              haven't even tried because "you're not full-stack".
            </p>
            <p>
              Want to build a <strong>modern app</strong> with multiplayer or
              offline-support? <strong>Figma, Notion and Linear</strong> all had
              to spend <strong>years</strong> on completely custom stacks.
            </p>
          </Prose>
        </div>
        <div className="flex flex-col gap-3 p-4 pt-8 md:p-8 md:gap-5">
          <Prose>
            <p className="font-display text-lg md:text-xl font-semibold text-stone-900 dark:text-white">
              What if we started from shared state?
            </p>
          </Prose>
          <div className="flex items-center flex-1">
            <DiagramAfterJazz className="mx-auto w-full h-auto max-w-sm" />
          </div>
          <Prose>
            <p>
              Jazz gives you <strong>local state</strong> that’s{" "}
              <strong>instantly synced and stored in the cloud.</strong>{" "}
              Including images and files.{" "}
              <strong>With users &amp; permissions built-in.</strong>
            </p>
            <p>
              With completely <strong>app-independent infra,</strong> you get to
              focus on <strong>building the app your users want.</strong> You'll
              notice that <strong>90% of the work is now the UI.</strong>
            </p>
          </Prose>
        </div>
      </div>
    </div>
  );
}
