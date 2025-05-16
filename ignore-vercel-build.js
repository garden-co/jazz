import { execSync } from "child_process";

const currentAppName =
  process.env.JAZZ_PROJECT_NAME || process.env.APP_NAME || process.argv[2];

try {
  // In Vercel CI, we need to use the remote cache and filter for the current app
  const previousSha = process.env.VERCEL_GIT_PREVIOUS_SHA;
  const currentSha = process.env.VERCEL_GIT_COMMIT_SHA;

  if (!previousSha || !currentSha) {
    console.log(
      `⚠️ Missing git SHA information. Proceeding with build for "${currentAppName}" to be safe.`,
    );
    process.exit(1);
  }

  // Use the previous deployment's SHA as our reference point
  const turboCommand = `pnpm turbo run build --dry=json --filter=${currentAppName}...[${previousSha}]`;
  const turboOutput = execSync(turboCommand).toString();

  const affectedPackages = JSON.parse(turboOutput).packages;
  const shouldBuild = affectedPackages.includes(currentAppName);

  if (shouldBuild) {
    console.log(
      `✅ Building "${currentAppName}" as it is affected by changes since previous deployment (${previousSha}).`,
    );
    process.exit(1); // Continue with the build
  } else {
    console.log(
      `🛑 Skipping build for "${currentAppName}" as it is not affected by changes since previous deployment (${previousSha}).`,
    );
    process.exit(0); // Skip the build
  }
} catch (error) {
  // If we can't parse the turbo output or something goes wrong,
  // we should build to be safe
  console.log(`⚠️ Error determining affected packages: ${error.message}`);
  console.log(`⚠️ Proceeding with build for "${currentAppName}" to be safe.`);
  process.exit(1);
}
