import { JazzLogo } from "@garden-co/design-system/src/components/atoms/logos/JazzLogo";
import { GcmpLogo } from "@garden-co/design-system/src/components/atoms/logos/GcmpLogo";
import type { StaticImageData } from "next/image";
import type { ReactNode } from "react";

interface IntroSlideProps {
  talkTitle: ReactNode;
  image?: StaticImageData | string;
  eventName: string;
  eventDate?: string;
}

export function IntroSlide({
  talkTitle,
  image,
  eventName,
  eventDate,
}: IntroSlideProps) {
  const imageSrc = image ? (typeof image === "string" ? image : image.src) : undefined;

  return (
    <div className="flex h-screen w-screen flex-col justify-between gap-5 p-20">
      <div className="flex justify-between">
        <JazzLogo className="h-20 self-start" />
        <div className="relative z-10 text-right text-2xl">
          <a
            href="https://jazz.tools"
            target="_blank"
            rel="noopener noreferrer"
          >
            jazz.tools
          </a>
          <br />
          <a
            href="https://x.com/jazz_tools"
            target="_blank"
            rel="noopener noreferrer"
          >
            @jazz_tools
          </a>
        </div>
      </div>

      <div className="-my-20 flex items-center justify-between">
        <h1 className="relative z-10 font-display text-8xl font-semibold tracking-tight">
          {talkTitle}
        </h1>

        {imageSrc && <img
          src={imageSrc}
          alt={eventName}
          className="w-[50%] opacity-50 invert"
        />}
      </div>

      <div className="relative z-10 flex items-center justify-between text-2xl">
        <GcmpLogo className="h-12" />
        <div className="text-center">
          Anselm Eickhoff
          <br />
          <a
            href="https://x.com/anselm_io"
            target="_blank"
            rel="noopener noreferrer"
          >
            @anselm_io
          </a>
        </div>
        <h2 className="text-right text-2xl">
          {eventName}
          {eventDate && (
            <>
              <br />
              {eventDate}
            </>
          )}
        </h2>
      </div>
    </div>
  );
}

