#!/usr/bin/env node

import { execSync } from "child_process";
import fs from "fs";
import path from "path";
import chalk from "chalk";
import { Command } from "commander";
import degit from "degit";
import inquirer from "inquirer";
import ora from "ora";

import {
  Framework,
  type FrameworkAuthPair,
  PLATFORM,
  frameworkToAuthExamples,
  frameworks,
} from "./config.js";

const program = new Command();

type PackageManager = "npm" | "yarn" | "pnpm" | "bun" | "deno";

type ScaffoldOptions = {
  template: FrameworkAuthPair | string;
  projectName: string;
  packageManager: PackageManager;
  apiKey?: string;
  git?: boolean;
};

type PromptOptions = {
  framework?: Framework;
  starter?: FrameworkAuthPair;
  example?: string;
  projectName?: string;
  packageManager?: PackageManager;
  apiKey?: string;
  git?: boolean;
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
          const data = (await response.json()) as { version: string };
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

function getPlatformFromTemplateName(template: string) {
  return template.includes("-rn") ? PLATFORM.REACT_NATIVE : PLATFORM.WEB;
}

// Function to check if the project is inside an existing git repository (monorepo)
function isInsideGitRepository(projectPath: string): boolean {
  try {
    const absolutePath = path.resolve(projectPath);

    // Check if .git exists in the current or any parent directory
    const result = execSync(`git rev-parse --is-inside-work-tree`, {
      cwd: absolutePath,
      stdio: "pipe",
      encoding: "utf-8",
    }).trim();

    return result === "true";
  } catch (error) {
    // If command fails, we're not in a git repo
    return false;
  }
}

async function scaffoldProject({
  template,
  projectName,
  packageManager,
  apiKey,
  git,
}: ScaffoldOptions): Promise<void> {
  const starterConfig = frameworkToAuthExamples[
    template as FrameworkAuthPair
  ] || {
    name: template,
    repo: "garden-co/jazz/examples/" + template,
    platform: getPlatformFromTemplateName(template),
  };

  const devCommand =
    starterConfig.platform === PLATFORM.REACT_NATIVE ? "ios" : "dev";

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

    // Helper function to update workspace dependencies
    async function updateWorkspaceDependencies(
      dependencyType: "dependencies" | "devDependencies",
    ) {
      if (packageJson[dependencyType]) {
        const latestVersions = await getLatestPackageVersions(
          packageJson[dependencyType],
        );

        Object.entries(packageJson[dependencyType]).forEach(
          ([pkg, version]) => {
            if (typeof version === "string" && version.includes("workspace:")) {
              packageJson[dependencyType][pkg] = latestVersions[pkg];
            }
          },
        );
      }
    }

    await Promise.all([
      updateWorkspaceDependencies("dependencies"),
      updateWorkspaceDependencies("devDependencies"),
    ]);

    // If projectName is ".", use the current directory name instead
    if (projectName === ".") {
      const currentDir = process.cwd().split("/").pop() || "jazz-app";
      packageJson.name = currentDir;
    } else {
      packageJson.name = projectName;
    }
    fs.writeFileSync(packageJsonPath, JSON.stringify(packageJson, null, 2));
    depsSpinner.succeed(chalk.green("Dependencies updated"));
  } catch (error) {
    depsSpinner.fail(chalk.red("Failed to update dependencies"));
    throw error;
  }

  // Replace API key if provided
  if (apiKey) {
    const replaceSpinner = ora({
      text: chalk.blue("Updating API key..."),
      spinner: "dots",
    }).start();

    try {
      const apiKeyPath = `${projectName}/src/apiKey.ts`;
      if (fs.existsSync(apiKeyPath)) {
        let content = fs.readFileSync(apiKeyPath, "utf8");
        // Replace the apiKey export value
        const keyPattern = /export const apiKey = ["']([^"']+)["']/;
        const updatedContent = content.replace(
          keyPattern,
          `export const apiKey = "${apiKey}"`,
        );

        if (content !== updatedContent) {
          fs.writeFileSync(apiKeyPath, updatedContent);
        }
      }
      replaceSpinner.succeed(chalk.green("API key updated"));
    } catch (error) {
      replaceSpinner.fail(chalk.red("Failed to update API key"));
      console.warn(
        chalk.yellow(
          "You may need to manually update the API key in your Jazz Provider.",
        ),
      );
    }
  }

  // Step 4: Install dependencies
  const installSpinner = ora({
    text: chalk.blue(
      `Installing dependencies with ${chalk.bold(packageManager)}...`,
    ),
    spinner: "dots",
  }).start();

  try {
    execSync(`cd "${projectName}" && ${packageManager} install`, {
      stdio: "pipe",
    });
    installSpinner.succeed(chalk.green("Dependencies installed"));
  } catch (error) {
    installSpinner.fail(chalk.red("Failed to install dependencies"));
    throw error;
  }

  // Additional setup for React Native
  if (starterConfig.platform === PLATFORM.REACT_NATIVE) {
    const rnSpinner = ora({
      text: chalk.blue("Setting up React Native project..."),
      spinner: "dots",
    }).start();

    try {
      execSync(`cd "${projectName}" && npx expo prebuild`, { stdio: "pipe" });
      execSync(`cd "${projectName}" && npx pod-install`, { stdio: "pipe" });

      // Update metro.config.js
      const metroConfigPath = `${projectName}/metro.config.js`;
      const metroConfig = `
const { getDefaultConfig } = require("expo/metro-config");
const { withNativeWind } = require("nativewind/metro");

const config = getDefaultConfig(__dirname);

module.exports = withNativeWind(config, { input: "./global.css" });
`;
      fs.writeFileSync(metroConfigPath, metroConfig);

      rnSpinner.succeed(chalk.green("React Native setup completed"));
    } catch (error) {
      rnSpinner.fail(chalk.red("Failed to setup React Native"));
      throw error;
    }
  }

  // Step 5: Clone cursor-docs
  const docsSpinner = ora({
    text: chalk.blue(`Adding .cursor directory...`),
    spinner: "dots",
  }).start();

  try {
    // Create a temporary directory for cursor-docs
    const tempDocsDir = `${projectName}-cursor-docs-temp`;
    const emitter = degit("garden-co/jazz/packages/cursor-docs", {
      cache: false,
      force: true,
      verbose: true,
    });

    // Clone cursor-docs to temp directory
    await emitter.clone(tempDocsDir);

    // Copy only the .cursor directory to project root
    const cursorDirSource = `${tempDocsDir}/.cursor`;
    const cursorDirTarget = `${projectName}/.cursor`;

    if (fs.existsSync(cursorDirSource)) {
      fs.cpSync(cursorDirSource, cursorDirTarget, { recursive: true });
      docsSpinner.succeed(chalk.green(".cursor directory added successfully"));
    } else {
      docsSpinner.fail(chalk.red(".cursor directory not found in cursor-docs"));
    }

    // Clean up temp directory
    fs.rmSync(tempDocsDir, { recursive: true, force: true });
  } catch (error) {
    docsSpinner.fail(chalk.red("Failed to add .cursor directory"));
    throw error;
  }

  // Step 6: Git init (conditionally)
  const gitSpinner = ora({
    text: chalk.blue("Checking git status..."),
    spinner: "dots",
  }).start();

  try {
    const projectPath = projectName === "." ? "." : projectName;

    if (isInsideGitRepository(projectPath)) {
      gitSpinner.info(
        chalk.yellow(
          "Project is inside an existing git repository (likely a monorepo). Skipping git initialization.",
        ),
      );
    } else {
      gitSpinner.stop();

      // Use git option if provided, otherwise prompt
      let initGit = git;

      if (initGit === undefined) {
        // Ask for confirmation
        const { confirmGitInit } = await inquirer.prompt([
          {
            type: "confirm",
            name: "confirmGitInit",
            message: chalk.cyan("Initialize git repository?"),
            default: true,
          },
        ]);
        initGit = confirmGitInit;
      }

      if (initGit) {
        const initSpinner = ora({
          text: chalk.blue("Initializing git repository..."),
          spinner: "dots",
        }).start();

        execSync(
          `cd "${projectName}" && git init && git add . && git commit -m "Initial commit from create-jazz-app"`,
          { stdio: "pipe" },
        );

        initSpinner.succeed(chalk.green("Git repository initialized"));
      } else {
        console.log(chalk.yellow("Git initialization skipped"));
      }
    }
  } catch (error) {
    gitSpinner.fail(chalk.red("Failed to check or initialize git repository"));
    console.error(error);
  }

  // Final success message
  console.log("\n" + chalk.green.bold("✨ Project setup completed! ✨\n"));
  console.log(chalk.cyan("To get started:"));

  // Skip the cd command if we're already in the project directory
  if (projectName !== ".") {
    console.log(chalk.white(`  cd ${chalk.bold(projectName)}`));
  }

  console.log(
    chalk.white(`  ${chalk.bold(`${packageManager} run ${devCommand}`)}\n`),
  );
}

async function promptUser(
  partialOptions: PromptOptions,
): Promise<ScaffoldOptions> {
  console.log(chalk.blue.bold("Let's create your Jazz app! 🎷\n"));

  const questions = [];

  if (
    partialOptions.framework &&
    !frameworks.find(
      (framework) => framework.value === partialOptions.framework,
    )
  ) {
    throw new Error(`Invalid framework: ${partialOptions.framework}`);
  }

  if (partialOptions.starter && partialOptions.example) {
    throw new Error("Please specify either a starter or an example, not both.");
  }

  if (!partialOptions.example && !partialOptions.starter) {
    let framework = partialOptions.framework;

    if (!partialOptions.framework) {
      const answers = await inquirer.prompt([
        {
          type: "list",
          name: "framework",
          message: chalk.cyan("Choose a framework:"),
          choices: Array.from(frameworks).map((framework) => ({
            name: chalk.white(framework.name),
            value: framework.value,
          })),
        },
      ]);
      framework = answers.framework;
    }

    let choices = Object.entries(frameworkToAuthExamples);

    if (framework) {
      choices = choices.filter(([key, value]) => key.startsWith(framework));
    }

    questions.push({
      type: "list",
      name: "starter",
      message: chalk.cyan("Choose an authentication method:"),
      choices: choices.map(([key, value]) => ({
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
    template:
      answers.starter || partialOptions.starter || partialOptions.example,
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

const frameworkOptions = frameworks.map((f) => f.name).join(", ");

program
  .description(
    chalk.blue("CLI to generate Jazz projects using starter templates"),
  )
  .option(
    "-f, --framework <framework>",
    chalk.cyan(`Framework to use (${frameworkOptions})`),
  )
  .option("-s, --starter <starter>", chalk.cyan("Starter template to use"))
  .option("-e, --example <name>", chalk.cyan("Example project to use"))
  .option(
    "-p, --package-manager <manager>",
    chalk.cyan("Package manager to use (npm, yarn, pnpm, bun, deno)"),
  )
  .option("-k, --api-key <key>", chalk.cyan("Jazz Cloud API key"))
  .option(
    "-g, --git <boolean>",
    chalk.cyan("Initialize git repository (true/false)"),
  )
  .argument(
    "[directory]",
    "Directory to create the project in (defaults to project name)",
  )
  .action(async (directory, options) => {
    try {
      const partialOptions: PromptOptions = {};

      if (options.starter && options.example) {
        throw new Error(
          chalk.red(
            "Cannot specify both starter and example. Please choose one.",
          ),
        );
      }

      // If directory is ".", set it as the project name or use it later
      if (directory === ".") {
        // Use current directory name as project name if not specified
        if (!options.projectName) {
          const currentDir = process.cwd().split("/").pop() || "jazz-app";
          partialOptions.projectName = currentDir;
        }
      } else if (directory && !options.projectName) {
        // If directory is provided but not project name, use directory as project name
        partialOptions.projectName = directory;
      }

      if (options.starter)
        partialOptions.starter = options.starter as FrameworkAuthPair;
      if (options.example) partialOptions.example = options.example;
      if (options.projectName) partialOptions.projectName = options.projectName;
      if (options.packageManager)
        partialOptions.packageManager =
          options.packageManager as ScaffoldOptions["packageManager"];
      if (options.framework) partialOptions.framework = options.framework;
      if (options.apiKey) partialOptions.apiKey = options.apiKey;

      // Parse git option
      if (options.git !== undefined) {
        if (options.git === "true" || options.git === true) {
          partialOptions.git = true;
        } else if (options.git === "false" || options.git === false) {
          partialOptions.git = false;
        }
      }

      // Get missing options through prompts
      const scaffoldOptions = await promptUser(partialOptions);

      // If directory is ".", we'll create the project in the current directory
      if (directory === ".") {
        scaffoldOptions.projectName = ".";
      }

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
  console.log(chalk.blue("\nAvailable starters:\n"));
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
    chalk.white("npx create-jazz-app@latest my-app --framework react\n"),
  );
  console.log(chalk.blue("With example app as a template:"));
  console.log(
    chalk.white("npx create-jazz-app@latest my-chat-app --example chat\n"),
  );
  console.log(chalk.blue("With API key:"));
  console.log(
    chalk.white(
      "npx create-jazz-app@latest my-app --api-key your-api-key@garden.co\n",
    ),
  );
  console.log(chalk.blue("Create in current directory:"));
  console.log(chalk.white("npx create-jazz-app@latest .\n"));

  console.log(chalk.blue("Disable git initialization:"));
  console.log(chalk.white("npx create-jazz-app@latest my-app --git false\n"));
});

program.parse(process.argv);
