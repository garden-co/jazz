import { Card } from "@garden-co/design-system/src/components/atoms/Card";
import { H3 } from "@garden-co/design-system/src/components/atoms/Headings";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";

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
      aria-hidden
      className="z-0 order-first p-4 text-sm relative overflow-hidden flex items-center md:py-0 md:justify-center md:order-last"
    >
      <div className="z-0 opacity-60 w-full h-full absolute top-0 -right-5 font-mono break-all text-stone-300 tracking-[0.5em] dark:text-stone-800">
        {randomChars.map((char, index) =>
          index % 2 === 0 ? (
            <span key={index}>{char}</span>
          ) : (
            <span key={index} className="text-stone-600 dark:text-muted">
              {char}
            </span>
          ),
        )}
      </div>

      <Icon
        name="encryption"
        size="3xl"
        className="z-30 p-1.5 rounded-lg"
        intent="primary"
        hasBackground
      />

      {/*<LockKeyholeIcon*/}
      {/*  strokeWidth={1.5}*/}
      {/*  strokeLinecap="butt"*/}
      {/*  size={80}*/}
      {/*  className="z-30 size-8 text-primary p-1.5 rounded-lg bg-highlight dark:bg-stone-900 md:size-10"*/}
      {/*/>*/}

      <div className="w-20 h-full bg-gradient-to-r from-white to-transparent absolute top-0 left-0 z-10 dark:from-stone-925"></div>
      <div className="hidden md:block h-20 w-full bg-gradient-to-b from-white to-transparent absolute top-0 left-0 z-10 dark:from-stone-925"></div>
      <div className="h-20 w-full bg-gradient-to-t from-white to-transparent absolute bottom-0 left-0 z-10 dark:from-stone-925"></div>
      <div className="w-20 h-full bg-gradient-to-l from-white to-transparent absolute top-0 right-0 z-10 dark:from-stone-925"></div>
    </div>
  );
}

export function EncryptionSection() {
  return (
    <Card className="overflow-hidden dark:bg-stone-925">
      <div className="grid md:grid-cols-3 md:gap-3">
        <div className="md:col-span-2 px-4 pb-4 md:p-8">
          <H3 className="mb-0 text-balance">
            End-to-end encrypted and tamper-proof
          </H3>

          <Prose className="max-w-2xl mt-2 md:mt-4">
            <p>
              <strong>
                The syncing server never sees your data in plaintext.
              </strong>{" "}
              Instead of trusting centralised authorisation, Jazz uses
              public-key cryptography. Your edits are encrypted and signed
              on-device, verifiable by everyone and readable only by those given
              access.
            </p>
          </Prose>
        </div>

        <Illustration />
      </div>
    </Card>
  );
}
