import type { IconName } from "@garden-co/design-system/src/components/atoms/Icon";
import { FeatureCard } from "@garden-co/design-system/src/components/molecules/FeatureCard";

const randomChars = [
  "SFPOHVKNPDKETOMQLMJKX#QDI=TFFFMRJDSJ",
  "A",
  "#MLZJJA-WJEATZULBR%I=MG#VUWOHX",
  "J",
  "HPLNSST!VOMKBANJTYRCR",
  "A",
  "SL#QN%YWI=QBHP-DBHN=A",
  "Z",
  "HXEHHJQJPXLWBI",
  "Z",
  "DPIXCSLHESD+TIVSPFISKG%LMPM",
  "J",
  "HYCSL#QN%IYPMPLQUKUJ",
  "A",
  "YTKAMZKIOD#YR",
  "Z",
  "SFPOHVKNPDKETOM",
  "Z",
  "VBXWFFIX+WVFRNM+CGT",
  "J",
  "HYCSL#QN%IYPMPLQUKUJ",
  "A",
  "KBANJTYRQ!OUTYAO",
  "Z",
];

function Illustration() {
  return (
    <div
      aria-hidden="true"
      className="pointer-events-none absolute inset-0 z-0 order-first flex select-none items-center overflow-hidden p-4 text-sm md:order-last md:justify-center md:py-0"
    >
      <div className="absolute -right-5 top-0 z-[-1] h-full w-full break-all font-mono tracking-[0.5em] text-stone-300 opacity-20 dark:text-stone-800">
        {randomChars.map((char, index) =>
          index % 2 === 0 ? (
            <span key={index}>{char}</span>
          ) : (
            <span key={index} className="text-stone-600 dark:text-muted">
              {char}
            </span>
          ),
        )}

        <div className="bg-linear-to-r absolute left-0 top-0 z-10 h-full w-20 from-white to-transparent dark:from-stone-925"></div>
        <div className="bg-linear-to-b absolute left-0 top-0 z-10 hidden h-20 w-full from-white to-transparent dark:from-stone-925 md:block"></div>
        <div className="bg-linear-to-t absolute bottom-0 left-0 z-10 h-20 w-full from-white to-transparent dark:from-stone-925"></div>
        <div className="bg-linear-to-l absolute right-0 top-0 z-10 h-full w-20 from-white to-transparent dark:from-stone-925"></div>
      </div>
    </div>
  );
}

export function LocalFirstFeaturesSection() {
  const features: Array<{
    title: string;
    icon: IconName;
    description: React.ReactNode;
  }> = [
    {
      title: "Offline-first",
      icon: "offline",
      description: (
        <>
          Your app works seamlessly offline or on sketchy connections. When
          you&apos;re back online, your data is synced.
        </>
      ),
    },
    {
      title: "Instant updates",
      icon: "instant",
      description: (
        <>
          Since you&apos;re working with local state, your UI updates instantly.
          Just mutate data. No API calls and spinners.
        </>
      ),
    },
    {
      title: "Real-Time Sync & Multiplayer",
      icon: "devices",
      description: (
        <>
          All devices and users stay perfectly in sync. Share data to enable
          live collaboration and presence UI like cursors.
        </>
      ),
    },
    {
      title: "Private by Design",
      icon: "encryption",
      description: (
        <>
          Encrypted and signed on your device. Invisible to servers, verifiable
          by anyone.
        </>
      ),
    },
  ];
  return (
    <div className="mb-12 grid gap-4 sm:grid-cols-2 md:gap-8 lg:mb-16 xl:grid-cols-4">
      {features.map(({ title, icon, description }) => (
        <FeatureCard
          label={title}
          icon={icon}
          explanation={description}
          key={title}
          className="relative"
        >
          {icon === "encryption" && <Illustration />}
        </FeatureCard>
      ))}
    </div>
  );
}
