import { JazzLogo } from "@/components/forMdx";
import { SimpleCentered } from "./Containers";
import { JazzSyncs } from "@/components/icons/JazzSyncs";

export function JustJazzLogoSlide() {
  return (
    <SimpleCentered>
      <JazzLogo className="h-20" />
    </SimpleCentered>
  );
}

export function SloganSlide() {
  return (
    <SimpleCentered>
      <JazzSyncs className="h-40" />
    </SimpleCentered>
  );
}

export function HowYouCouldHaveInventedJazz() {
  return (
    <div className="flex h-screen w-screen flex-col items-center justify-center gap-5 p-20">
      <h2 className="font-display text-5xl font-semibold tracking-tight">
        “How you could have invented <JazzLogo className="inline-block h-16" />{" "}
        in an afternoon”
      </h2>
    </div>
  );
}