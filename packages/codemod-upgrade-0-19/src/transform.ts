import {
  Project,
  SyntaxKind,
  SourceFile,
  CallExpression,
  PropertyAccessExpression,
  ImportDeclaration,
} from "ts-morph";
import fs from "node:fs";

export function transformFile(sourceFile: SourceFile): string {
  renameHooks(sourceFile);

  return sourceFile.getFullText();
}

/**
 * Renames useAccountWithSelector to useAccount and useCoStateWithSelector to useCoState
 */
function renameHooks(sourceFile: SourceFile) {
  transformHookImports(sourceFile);
  transformHookCalls(sourceFile);
}

function transformHookImports(sourceFile: SourceFile) {
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.ImportDeclaration) {
      const importDecl = node as ImportDeclaration;
      const moduleSpecifier = importDecl.getModuleSpecifierValue();

      // Only process imports from jazz-tools packages
      if (moduleSpecifier.includes("jazz-tools")) {
        const namedImports = importDecl.getNamedImports();

        namedImports.forEach((namedImport) => {
          const importName = namedImport.getName();
          if (importName === "useCoStateWithSelector") {
            namedImport.setName("useCoState");
          }
          if (importName === "useAccountWithSelector") {
            namedImport.setName("useAccount");
          }
        });
      }
    }
  });
}

function transformHookCalls(sourceFile: SourceFile) {
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.CallExpression) {
      const callExpression = node as CallExpression;
      const expression = callExpression.getExpression();

      // Handle direct function calls
      if (expression.getKind() === SyntaxKind.Identifier) {
        const functionName = expression.getText();

        if (functionName === "useCoStateWithSelector") {
          expression.replaceWithText("useCoState");
        } else if (functionName === "useAccountWithSelector") {
          expression.replaceWithText("useAccount");
        }
      }
    }
  });
}

// Main function to run the transform
function singleRun(projectPath: string) {
  let project: Project;

  if (fs.existsSync(`${projectPath}/tsconfig.json`)) {
    project = new Project({
      tsConfigFilePath: `${projectPath}/tsconfig.json`,
    });
  } else {
    project = new Project();

    const supportedExtensions = [".ts", ".tsx"];
    const hasSupportedExtension = supportedExtensions.some((ext) =>
      projectPath.endsWith(ext),
    );

    if (hasSupportedExtension) {
      project.addSourceFilesAtPaths(projectPath);
    } else {
      project.addSourceFilesAtPaths(`${projectPath}/**/*.{ts,tsx}`);
    }
  }

  const sourceFiles = project.getSourceFiles();

  let changed = false;

  sourceFiles.forEach((sourceFile) => {
    if (sourceFile.getFilePath().includes("node_modules")) {
      return;
    }

    try {
      const originalText = sourceFile.getFullText();
      const transformedText = transformFile(sourceFile);

      if (originalText !== transformedText) {
        sourceFile.saveSync();
        console.log(`Transformed: ${sourceFile.getFilePath()}`);
        changed = true;
      }
    } catch (error) {
      console.error(`Error transforming ${sourceFile.getFilePath()}:`, error);
    }
  });

  return changed;
}

// Run multiple times, because the resolved types gets fixed after each run
export function runTransform(projectPath: string): void {
  let runCount = 1;

  while (singleRun(projectPath)) {
    runCount++;

    if (runCount > 5) {
      console.log("Ran 5 times, stopping");
      break;
    }
  }

  console.log(`Ran ${runCount} times`);
}
