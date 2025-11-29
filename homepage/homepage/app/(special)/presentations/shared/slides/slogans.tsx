import { JazzLogo } from "@/components/forMdx";

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