import { clsx } from "clsx";
import { Card } from "gcmp-design-system/src/app/components/atoms/Card";
import { H2 } from "gcmp-design-system/src/app/components/atoms/Headings";
import { GappedGrid } from "gcmp-design-system/src/app/components/molecules/GappedGrid";
import CodeStepAction from "./CodeStepAction.mdx";
import CodeStepCloud from "./CodeStepCloud.mdx";
import CodeStepRender from "./CodeStepRender.mdx";
import CodeStepSchema from "./CodeStepSchema.mdx";

function Code({
  children,
  className,
  fileName,
}: {
  children: React.ReactNode;
  className?: string;
  fileName?: string;
}) {
  return (
    <div
      className={clsx(
        className,
        "w-full h-full relative -right-2 -bottom-1 max-w-full lg:max-w-[480px] overflow-x-auto ml-auto overflow-hidden",
        "shadow-xl shadow-blue/20 ",
        "rounded-tl-lg border",
        "flex-1 bg-white ring ring-4 ring-stone-400/20",
        "dark:bg-stone-925",
      )}
    >
      <div className="flex px-4 border-b">
        <span className="text-xs lg:text-sm border-b border-blue py-2 dark:border-blue-400">
          {fileName}
        </span>
      </div>
      <pre className="text-xs lg:text-sm p-1 pb-2">{children}</pre>
    </div>
  );
}

function Step({
  step,
  description,
  children,
  className,
}: {
  step: number;
  description: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <Card
      className={clsx(
        className,
        "overflow-hidden flex flex-col gap-6",
        "pt-4 sm:pt-6",
        "col-span-2 lg:col-span-3",
      )}
    >
      <div className="flex gap-3 px-4 sm:px-6">
        <p
          className={clsx(
            "bg-blue-50 size-6 rounded-full text-blue text-sm font-semibold font-mono",
            "inline-flex items-center justify-center text-center shrink-0",
            "dark:bg-blue dark:text-white",
          )}
        >
          <span className="sr-only">Step</span>
          {step}
        </p>
        <p className="max-w-md">{description}</p>
      </div>
      <div className="flex-1 pl-4 sm:pl-12">{children}</div>
    </Card>
  );
}

export function HowJazzWorksSection() {
  const imageProps = {
    alt: "Code samples for defining a schema for Jazz, pushing data, and subscribing to changes.",
    width: 1100,
    height: 852,
  };

  return (
    <div className="grid gap-8">
      <div className="grid gap-3">
        <p className="uppercase text-blue tracking-widest text-sm font-medium dark:text-stone-400">
          Collaborative Values
        </p>

        <H2>Build entire apps using only client-side code</H2>
      </div>
      <GappedGrid>
        <Step
          step={1}
          description="Define your schema using Collaborative Values &mdash; your new building blocks."
        >
          <Code fileName="schema.ts">
            <CodeStepSchema />
          </Code>
        </Step>
        <Step
          step={2}
          description="Connect to sync and storage infrastructure — Jazz Cloud or self-hosted."
        >
          <Code fileName="main.tsx">
            <CodeStepCloud />
          </Code>
        </Step>
        <Step
          step={3}
          description="Create a Collaborative Value, and it will be synced and persisted automatically."
        >
          <Code fileName="sendMessage.ts">
            <CodeStepAction />
          </Code>
        </Step>
        <Step
          step={4}
          description="Read your data like simple local state. Get instant sync and UI updates across all devices and users. 🎉"
        >
          <Code fileName="ChatScreen.tsx">
            <CodeStepRender />
          </Code>
        </Step>
      </GappedGrid>
    </div>
  );
}
