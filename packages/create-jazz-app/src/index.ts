#!/usr/bin/env node

import { execSync } from "child_process";
import fs from "fs";
import chalk from "chalk";
import { Command } from "commander";
import degit from "degit";
import gradient from "gradient-string";
import inquirer from "inquirer";
import ora from "ora";

import { type FrameworkAuthPair, frameworkToAuthExamples } from "./config.js";

const program = new Command();

const jazzGradient = gradient(["#FF4D4D", "#FF9900", "#FFD700"]);

type PackageManager = "npm" | "yarn" | "pnpm" | "bun" | "deno";

type ScaffoldOptions = {
  template: FrameworkAuthPair | string;
  projectName: string;
  packageManager: PackageManager;
};

type PromptOptions = {
  starter?: FrameworkAuthPair;
  example?: string;
  projectName?: string;
  packageManager?: PackageManager;
};

async function getLatestPackageVersions(
  dependencies: Record<string, string>,
): Promise<Record<string, string>> {
  const versionsSpinner = ora({
    text: chalk.blue("Fetching package versions..."),
    spinner: "dots",
  }).start();

  const versions: Record<string, string> = {};
  const failures: string[] = [];

  await Promise.all(
    Object.keys(dependencies).map(async (pkg) => {
      if (
        typeof dependencies[pkg] === "string" &&
        dependencies[pkg].includes("workspace:")
      ) {
        try {
          const response = await fetch(
            `https://registry.npmjs.org/${pkg}/latest`,
          );
          if (!response.ok) throw new Error(`HTTP ${response.status}`);
          const data = await response.json();
          versions[pkg] = `^${data.version}`; // Using caret for minor version updates
        } catch (error) {
          failures.push(pkg);
        }
      }
    }),
  );

  if (failures.length > 0) {
    versionsSpinner.fail(
      chalk.red(
        `Failed to fetch versions for packages: ${failures.join(", ")}. Please check your internet connection and try again.`,
      ),
    );
    throw new Error("Failed to fetch package versions");
  }

  versionsSpinner.succeed(chalk.green("Package versions fetched successfully"));
  return versions;
}

async function scaffoldProject({
  template,
  projectName,
  packageManager,
}: ScaffoldOptions): Promise<void> {
  console.log("\n" + jazzGradient.multiline("Jazz App Creator\n"));

  const starterConfig = frameworkToAuthExamples[
    template as FrameworkAuthPair
  ] || { name: template, repo: "garden-co/jazz/examples/" + template };
  if (!starterConfig) {
    throw new Error(`Invalid template: ${template}`);
  }

  const devCommand =
    template === "react-native-expo-clerk-auth" ? "ios" : "dev";

  if (!starterConfig.repo) {
    throw new Error(
      `Starter template ${starterConfig.name} is not yet implemented`,
    );
  }

  // Step 2: Clone starter
  const cloneSpinner = ora({
    text: chalk.blue(`Cloning template: ${chalk.bold(starterConfig.name)}`),
    spinner: "dots",
  }).start();

  try {
    const emitter = degit(starterConfig.repo, {
      cache: false,
      force: true,
      verbose: true,
    });
    await emitter.clone(projectName);
    cloneSpinner.succeed(chalk.green("Template cloned successfully"));
  } catch (error) {
    cloneSpinner.fail(chalk.red("Failed to clone template"));
    throw error;
  }

  // Step 3: Fixing dependencies
  const depsSpinner = ora({
    text: chalk.blue("Updating dependencies..."),
    spinner: "dots",
  }).start();

  try {
    const packageJsonPath = `${projectName}/package.json`;
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

    // Replace workspace: dependencies with latest
    if (packageJson.dependencies) {
      const latestVersions = await getLatestPackageVersions(
        packageJson.dependencies,
      );

      Object.entries(packageJson.dependencies).forEach(([pkg, version]) => {
        if (typeof version === "string" && version.includes("workspace:")) {
          packageJson.dependencies[pkg] = latestVersions[pkg];
        }
      });
    }

    packageJson.name = projectName;
    fs.writeFileSync(packageJsonPath, JSON.stringify(packageJson, null, 2));
    depsSpinner.succeed(chalk.green("Dependencies updated"));
  } catch (error) {
    depsSpinner.fail(chalk.red("Failed to update dependencies"));
    throw error;
  }

  // Step 4: Install dependencies
  const installSpinner = ora({
    text: chalk.blue(
      `Installing dependencies with ${chalk.bold(packageManager)}...`,
    ),
    spinner: "dots",
  }).start();

  try {
    execSync(`cd ${projectName} && ${packageManager} install`, {
      stdio: "pipe",
    });
    installSpinner.succeed(chalk.green("Dependencies installed"));
  } catch (error) {
    installSpinner.fail(chalk.red("Failed to install dependencies"));
    throw error;
  }

  // Additional setup for React Native
  if (template === "react-native-expo-clerk-auth") {
    const rnSpinner = ora({
      text: chalk.blue("Setting up React Native project..."),
      spinner: "dots",
    }).start();

    try {
      execSync(`cd ${projectName} && npx expo prebuild`, { stdio: "pipe" });
      execSync(`cd ${projectName} && npx pod-install`, { stdio: "pipe" });

      // Update metro.config.js
      const metroConfigPath = `${projectName}/metro.config.js`;
      const metroConfig = `
const { getDefaultConfig } = require("expo/metro-config");
const { withNativeWind } = require("nativewind/metro");

const config = getDefaultConfig(__dirname);

config.resolver.unstable_enablePackageExports = true;

module.exports = withNativeWind(config, { input: "./src/global.css" });
`;
      fs.writeFileSync(metroConfigPath, metroConfig);

      rnSpinner.succeed(chalk.green("React Native setup completed"));
    } catch (error) {
      rnSpinner.fail(chalk.red("Failed to setup React Native"));
      throw error;
    }
  }

  // Final success message
  console.log("\n" + chalk.green.bold("✨ Project setup completed! ✨\n"));
  console.log(chalk.cyan("To get started:"));
  console.log(chalk.white(`  cd ${chalk.bold(projectName)}`));
  console.log(
    chalk.white(`  ${chalk.bold(`${packageManager} run ${devCommand}`)}\n`),
  );
}

async function promptUser(
  partialOptions: PromptOptions,
): Promise<ScaffoldOptions> {
  console.log("\n" + jazzGradient.multiline("Jazz App Creator\n"));
  console.log(chalk.blue.bold("Let's create your Jazz app! 🎷\n"));

  const questions = [];

  if (partialOptions.starter && partialOptions.example) {
    throw new Error("Please specify either a starter or an example, not both.");
  }

  if (!partialOptions.example && !partialOptions.starter) {
    questions.push({
      type: "list",
      name: "starter",
      message: chalk.cyan("Choose a starter:"),
      choices: Object.entries(frameworkToAuthExamples)
        .filter(([_, value]) => value.repo) // Only show implemented starters
        .map(([key, value]) => ({
          name: chalk.white(value.name),
          value: key,
        })),
    });
  }

  if (!partialOptions.packageManager) {
    questions.push({
      type: "list",
      name: "packageManager",
      message: chalk.cyan("Choose a package manager:"),
      choices: [
        { name: chalk.white("npm"), value: "npm" },
        { name: chalk.white("yarn"), value: "yarn" },
        { name: chalk.white("pnpm"), value: "pnpm" },
        { name: chalk.white("bun"), value: "bun" },
        { name: chalk.white("deno"), value: "deno" },
      ],
      default: "npm",
    });
  }

  if (!partialOptions.projectName) {
    questions.push({
      type: "input",
      name: "projectName",
      message: chalk.cyan("Enter your project name:"),
      validate: (input: string) =>
        input ? true : chalk.red("Project name cannot be empty"),
    });
  }

  const answers = await inquirer.prompt(questions);

  return {
    ...answers,
    ...partialOptions,
    template: partialOptions.starter || partialOptions.example,
  } as ScaffoldOptions;
}

function validateOptions(options: PromptOptions): options is ScaffoldOptions {
  const errors: string[] = [];

  if (!options.starter && !options.example) {
    errors.push("Starter or example template is required");
  }
  if (!options.projectName) {
    errors.push("Project name is required");
  }
  if (!options.packageManager) {
    errors.push("Package manager is required");
  }

  if (options.starter && !frameworkToAuthExamples[options.starter]) {
    errors.push(`Invalid starter template: ${options.starter}`);
  }

  if (
    options.packageManager &&
    !["npm", "yarn", "pnpm", "bun", "deno"].includes(options.packageManager)
  ) {
    errors.push(`Invalid package manager: ${options.packageManager}`);
  }

  if (errors.length > 0) {
    throw new Error(chalk.red(errors.join("\n")));
  }

  return true;
}

program
  .description(chalk.blue("CLI to generate Jazz starter projects"))
  .option("-s, --starter <starter>", chalk.cyan("Starter template to use"))
  .option("-e, --example <name>", chalk.cyan("Example project to use"))
  .option("-n, --project-name <name>", chalk.cyan("Name of the project"))
  .option(
    "-p, --package-manager <manager>",
    chalk.cyan("Package manager to use (npm, yarn, pnpm, bun, deno)"),
  )
  .action(async (options) => {
    try {
      const partialOptions: PromptOptions = {};

      if (options.starter && options.example) {
        throw new Error(
          chalk.red(
            "Cannot specify both starter and example. Please choose one.",
          ),
        );
      }

      if (options.starter)
        partialOptions.starter = options.starter as FrameworkAuthPair;
      if (options.example) partialOptions.example = options.example;
      if (options.projectName) partialOptions.projectName = options.projectName;
      if (options.packageManager)
        partialOptions.packageManager =
          options.packageManager as ScaffoldOptions["packageManager"];

      // Get missing options through prompts
      const scaffoldOptions = await promptUser(partialOptions);

      // Validate will throw if invalid
      validateOptions(scaffoldOptions);
      await scaffoldProject(scaffoldOptions);
    } catch (error: any) {
      if (error instanceof Error && error.name === "ExitPromptError") {
        console.log(chalk.yellow("\n👋 Until next time!\n"));
      } else {
        console.error(chalk.red("\n❌ Error:"), error.message, "\n");
        process.exit(1);
      }
    }
  });

// Add help text to show available starters
program.on("--help", () => {
  console.log("\n" + jazzGradient.multiline("Available starters:\n"));
  Object.entries(frameworkToAuthExamples).forEach(([key, value]) => {
    if (value.repo) {
      // Only show implemented starters
      console.log(
        chalk.cyan(`  ${key}`),
        chalk.white("-"),
        chalk.white(value.name),
      );
    }
  });
  console.log(chalk.blue("\nExample usage:"));
  console.log(
    chalk.white(
      "  create-jazz-app --starter react-demo-auth --project-name my-app --package-manager npm\n",
    ),
  );
});

program.parse(process.argv);
