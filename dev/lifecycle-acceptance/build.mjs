// Builds the backend and one language's consumer from a fetched candidate.
// usage: node build.mjs rust|swift|kotlin|go
// Env: LA_WORK (outside any checkout), LA_SOURCE (the candidate's checkout: rust and go).
import { assertOutsideCheckouts } from "./build/consumer.mjs";
import { LANGS, die, timed } from "./lib/common.mjs";
import { installNpm } from "./build/npm.mjs";

const lang = process.argv[2];
try {
  if (!LANGS.includes(lang)) throw new Error(`usage: build.mjs ${LANGS.join("|")}`);
  assertOutsideCheckouts();
  await installNpm();
  const build = (await import(`./build/${lang}.mjs`))[`build${lang[0].toUpperCase()}${lang.slice(1)}`];
  await timed(`${lang} consumer total`, build);
  console.log(`BUILT ${lang}`);
} catch (error) {
  die(`build ${lang}: ${error.message}`);
}
