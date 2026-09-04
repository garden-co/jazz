import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

export function assertFixtureNetworkPolicy(manifest, policy) {
  const stripComments = (xml) => xml.replace(/<!--[\s\S]*?-->/g, "");
  const application = stripComments(manifest).match(/<application\b[^>]*>/)?.[0] ?? "";
  if (!/android:networkSecurityConfig="@xml\/jazz_device_network_security"/.test(application))
    throw new Error("release merged manifest does not select the fixture network policy");
  const xml = stripComments(policy);
  if (
    !/<base-config\s+cleartextTrafficPermitted="false"\s*\/>/.test(xml) ||
    !/<domain-config\s+cleartextTrafficPermitted="true">\s*<domain\s+includeSubdomains="false">10\.0\.2\.2<\/domain>\s*<\/domain-config>/.test(xml) ||
    (xml.match(/<domain-config\b/g) ?? []).length !== 1 ||
    (xml.match(/<domain\s/g) ?? []).length !== 1
  ) throw new Error("fixture network policy must allow only the emulator host");
}

/** Inspect the actual release merge output, not the debug manifest overlay.
 * The native request also checks NetworkSecurityPolicy in the installed app. */
export function verifyAndroidReleaseNetworkPolicy(projectRoot) {
  const mergedRoot = join(projectRoot, "app/build/intermediates/merged_manifests/release");
  const manifests = readdirSync(mergedRoot, { recursive: true }).filter((path) => path.endsWith("AndroidManifest.xml"));
  if (manifests.length !== 1) throw new Error("expected one merged release Android manifest");
  assertFixtureNetworkPolicy(
    readFileSync(join(mergedRoot, manifests[0]), "utf8"),
    readFileSync(join(projectRoot, "app/src/main/res/xml/jazz_device_network_security.xml"), "utf8"),
  );
}
