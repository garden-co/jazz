"use client";

import { heroCopy } from "@/content/homepage";
import { H1 } from "@garden-co/design-system/src/components/atoms/Headings";
import { Kicker } from "@garden-co/design-system/src/components/atoms/Kicker";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";

export function HeroSection() {
  return (
    <div className="container grid items-center gap-x-8 gap-y-12 mt-12 md:mt-16 lg:mt-24 mb-12 lg:gap-x-10 lg:grid-cols-12">
      <div className="flex flex-col justify-center gap-5 lg:col-span-11 lg:gap-8">
        <Kicker>{heroCopy.kicker}</Kicker>
        <H1>
          <span className="inline-block text-highlight">
            {heroCopy.headline}
          </span>
        </H1>

        <Prose size="lg" className="text-pretty max-w-2xl dark:text-stone-200 prose-p:leading-normal">
          {heroCopy.descriptionLong}
        </Prose>
      </div>
    </div>
  );
}
