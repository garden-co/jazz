import {
  Project,
  SyntaxKind,
  SourceFile,
  CallExpression,
  PropertyAccessExpression,
  ImportDeclaration,
  VariableDeclaration,
  Node,
  Type,
  Symbol,
} from "ts-morph";
import fs from "node:fs";

export function transformFile(sourceFile: SourceFile): string {
  migrateOnError(sourceFile);

  migrateUseAccount(sourceFile);

  renameWithSelectorHooks(sourceFile);

  migrateLoadingStateHandling(sourceFile);

  migrateMaybeLoadedIfStatements(sourceFile);

  return sourceFile.getFullText();
}

function migrateOnError(sourceFile: SourceFile) {
  const fullText = sourceFile.getFullText();

  // Simple text replacement: $onError: null -> $onError: 'catch'
  const newText = fullText.replace(/\$onError:\s*null/g, "$onError: 'catch'");

  if (newText !== fullText) {
    sourceFile.replaceWithText(newText);
  }
}

function migrateUseAccount(sourceFile: SourceFile) {
  const fullText = sourceFile.getFullText();

  const replacementsToMake: Array<{
    start: number;
    end: number;
    replacement: string;
  }> = [];

  // Track non-destructured useAccount calls to replace property accesses
  const nonDestructuredAccounts: Array<{
    varName: string;
    needsAgent: boolean;
    needsLogOut: boolean;
    variableStatement: any;
  }> = [];

  // Find all useAccount calls
  sourceFile.forEachDescendant((node) => {
    if (Node.isVariableDeclaration(node)) {
      const initializer = node.getInitializer();
      if (
        initializer &&
        Node.isCallExpression(initializer) &&
        initializer.getExpression().getText() === "useAccount"
      ) {
        const nameNode = node.getNameNode();

        // Handle destructuring pattern
        if (Node.isObjectBindingPattern(nameNode)) {
          const properties = new Map<string, string>();
          nameNode.getElements().forEach((element) => {
            const propertyName =
              element.getPropertyNameNode()?.getText() || element.getName();
            const localName = element.getName();
            properties.set(propertyName, localName);
          });

          if (properties.size > 0) {
            const variableStatement = node.getVariableStatement();
            if (variableStatement) {
              const declarationKind = variableStatement.getDeclarationKind();
              const useAccountCall = initializer.getText();

              // Get the statement's position in the source
              const statementStart = variableStatement.getStart();
              const statementEnd = variableStatement.getEnd();

              // Calculate indentation - look back from statement start to find the line start
              let lineStart = statementStart;
              while (lineStart > 0 && fullText[lineStart - 1] !== "\n") {
                lineStart--;
              }
              const indentation = fullText.substring(lineStart, statementStart);

              // Build replacement statements
              const statementsToInsert: string[] = [];

              if (properties.has("me")) {
                const meName = properties.get("me")!;
                statementsToInsert.push(
                  `${indentation}${declarationKind} ${meName} = ${useAccountCall};`,
                );
              }

              if (properties.has("agent")) {
                const agentName = properties.get("agent")!;
                statementsToInsert.push(
                  `${indentation}${declarationKind} ${agentName} = useAgent();`,
                );
              }

              if (properties.has("logOut")) {
                const logOutName = properties.get("logOut")!;
                statementsToInsert.push(
                  `${indentation}${declarationKind} ${logOutName} = useLogOut();`,
                );
              }

              const replacement = statementsToInsert.join("\n");

              // Replace from the start of the indentation to the end of the statement
              replacementsToMake.push({
                start: lineStart,
                end: statementEnd,
                replacement,
              });
            }
          }
        } else if (Node.isIdentifier(nameNode)) {
          // Handle non-destructured pattern: const account = useAccount()
          const varName = nameNode.getText();
          const variableStatement = node.getVariableStatement();
          if (variableStatement) {
            nonDestructuredAccounts.push({
              varName,
              needsAgent: false,
              needsLogOut: false,
              variableStatement,
            });
          }
        }
      }
    }
  });

  // Find all property accesses on non-destructured accounts
  for (const info of nonDestructuredAccounts) {
    const varName = info.varName;
    const scope = info.variableStatement.getParent();

    scope.forEachDescendant((node: Node) => {
      if (Node.isPropertyAccessExpression(node)) {
        const expression = node.getExpression();

        // Check if this is a direct property access on our variable (e.g., account.me, account.agent)
        if (Node.isIdentifier(expression) && expression.getText() === varName) {
          const propertyName = node.getName();

          if (propertyName === "me") {
            // Replace account.me with just account
            // But we need to replace only the .me part, keeping any chained access
            // For "account.me?.profile", we want to replace "account.me" with "account"
            replacementsToMake.push({
              start: expression.getEnd(), // Start after "account"
              end: node.getEnd(), // End after "me"
              replacement: "", // Remove ".me"
            });
          } else if (propertyName === "agent") {
            info.needsAgent = true;
            // Replace account.agent with agent
            replacementsToMake.push({
              start: node.getStart(),
              end: node.getEnd(),
              replacement: "agent",
            });
          } else if (propertyName === "logOut") {
            info.needsLogOut = true;
            // Replace account.logOut with logOut
            replacementsToMake.push({
              start: node.getStart(),
              end: node.getEnd(),
              replacement: "logOut",
            });
          }
        }
      }
    });

    // Add new variable declarations for agent/logOut if needed
    if (info.needsAgent || info.needsLogOut) {
      const variableStatement = info.variableStatement;
      const statementStart = variableStatement.getStart();
      const statementEnd = variableStatement.getEnd();

      // Calculate indentation
      let lineStart = statementStart;
      while (lineStart > 0 && fullText[lineStart - 1] !== "\n") {
        lineStart--;
      }
      const indentation = fullText.substring(lineStart, statementStart);

      const declarationKind = variableStatement.getDeclarationKind();
      const statementsToInsert: string[] = [];

      // Keep the original useAccount line
      statementsToInsert.push(fullText.substring(lineStart, statementEnd));

      if (info.needsAgent) {
        statementsToInsert.push(
          `${indentation}${declarationKind} agent = useAgent();`,
        );
      }

      if (info.needsLogOut) {
        statementsToInsert.push(
          `${indentation}${declarationKind} logOut = useLogOut();`,
        );
      }

      const replacement = statementsToInsert.join("\n");
      replacementsToMake.push({
        start: lineStart,
        end: statementEnd,
        replacement,
      });
    }
  }

  if (replacementsToMake.length === 0) {
    return;
  }

  // Apply replacements in reverse order to avoid position shifts
  // IMPORTANT: Do this BEFORE adding imports, otherwise positions will be shifted!
  replacementsToMake.sort((a, b) => b.start - a.start);

  for (const { start, end, replacement } of replacementsToMake) {
    sourceFile.replaceText([start, end], replacement);
  }

  // Now add necessary imports (after replacements so we don't shift positions)
  let needsUseAgentImport = false;
  let needsUseLogOutImport = false;

  for (const r of replacementsToMake) {
    if (r.replacement.includes("useAgent()")) {
      needsUseAgentImport = true;
    }
    if (r.replacement.includes("useLogOut()")) {
      needsUseLogOutImport = true;
    }
  }

  if (needsUseAgentImport || needsUseLogOutImport) {
    addJazzToolsImports(sourceFile, needsUseAgentImport, needsUseLogOutImport);
  }
}

function addJazzToolsImports(
  sourceFile: SourceFile,
  needsUseAgent: boolean,
  needsUseLogOut: boolean,
) {
  const jazzToolsImport = sourceFile
    .getImportDeclarations()
    .find((importDecl) => {
      const moduleSpecifier = importDecl.getModuleSpecifierValue();
      const namedImports = importDecl.getNamedImports();
      const existingImportNames = new Set(
        namedImports.map((ni) => ni.getName()),
      );
      return (
        moduleSpecifier.includes("jazz-tools") &&
        existingImportNames.has("useAccount")
      );
    });

  if (!jazzToolsImport) {
    return;
  }

  const namedImports = jazzToolsImport.getNamedImports();
  const existingImportNames = new Set(namedImports.map((ni) => ni.getName()));

  if (needsUseAgent && !existingImportNames.has("useAgent")) {
    jazzToolsImport.addNamedImport("useAgent");
  }

  if (needsUseLogOut && !existingImportNames.has("useLogOut")) {
    jazzToolsImport.addNamedImport("useLogOut");
  }
}

/**
 * Renames useAccountWithSelector to useAccount and useCoStateWithSelector to useCoState
 */
function renameWithSelectorHooks(sourceFile: SourceFile) {
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

        const existingImportNames = new Set(
          namedImports.map((ni) => ni.getName()),
        );

        namedImports.forEach((namedImport) => {
          const importName = namedImport.getName();
          if (importName === "useCoStateWithSelector") {
            if (existingImportNames.has("useCoState")) {
              namedImport.remove();
            } else {
              namedImport.setName("useCoState");
            }
          }
          if (importName === "useAccountWithSelector") {
            if (existingImportNames.has("useAccount")) {
              namedImport.remove();
            } else {
              namedImport.setName("useAccount");
            }
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

function migrateLoadingStateHandling(sourceFile: SourceFile) {
  const fullText = sourceFile.getFullText();

  // Find all useAccount and useCoState calls
  sourceFile.forEachDescendant((node) => {
    if (Node.isCallExpression(node)) {
      const expression = node.getExpression();
      if (!Node.isIdentifier(expression)) return;

      const functionName = expression.getText();
      if (functionName !== "useAccount" && functionName !== "useCoState") {
        return;
      }

      // Find the options argument
      // useAccount(Schema, options) - 2 args, options is args[1]
      // useCoState(Schema, id, options) - 3 args, options is args[2]
      const args = node.getArguments();
      let optionsArg = args.find((arg) => Node.isObjectLiteralExpression(arg));

      // Determine the parameter name from existing selector, variable name, or use default
      let paramName = functionName === "useAccount" ? "account" : "value";

      // Try to get the variable name from the declaration
      const parent = node.getParent();
      if (Node.isVariableDeclaration(parent)) {
        const nameNode = parent.getNameNode();
        if (Node.isIdentifier(nameNode)) {
          paramName = nameNode.getText();
        }
      }

      // Handle case where there's no options argument
      if (!optionsArg) {
        // Add options argument with select property
        const newSelector = `(${paramName}) => ${paramName}.$isLoaded ? ${paramName} : ${paramName}.$jazz.loadingState === "loading" ? undefined : null`;

        if (functionName === "useAccount") {
          // For useAccount:
          // useAccount() -> useAccount(undefined, { select: ... })
          // useAccount(Schema) -> useAccount(Schema, { select: ... })

          if (args.length === 0) {
            // No arguments at all, add undefined as schema
            node.addArgument("undefined");
          }
          // Add options as second argument
          node.addArgument(`{ select: ${newSelector} }`);
        } else if (functionName === "useCoState") {
          // For useCoState, we need: useCoState(Schema, id) -> useCoState(Schema, id, { select: ... })
          node.addArgument(`{ select: ${newSelector} }`);
        }
        return;
      }

      // Find the select property
      const selectProperty = optionsArg
        .getProperties()
        .find(
          (prop) =>
            Node.isPropertyAssignment(prop) && prop.getName() === "select",
        );

      if (selectProperty && Node.isPropertyAssignment(selectProperty)) {
        // Hook WITH existing selector
        const initializer = selectProperty.getInitializer();
        if (!initializer) {
          return;
        }

        // Extract parameter name from arrow function or function expression
        if (
          Node.isArrowFunction(initializer) ||
          Node.isFunctionExpression(initializer)
        ) {
          const params = initializer.getParameters();
          if (params.length > 0) {
            paramName = params[0].getName();
          }

          // Get the body of the selector
          const body = initializer.getBody();
          let selectorBody: string;

          if (Node.isBlock(body)) {
            // Function body with braces
            selectorBody = body.getFullText();
          } else {
            // Arrow function without braces
            selectorBody = body.getText();
          }

          // Remove optional chaining from the selector body since we're checking $isLoaded
          // Convert account?.profile?.name to account.profile.name
          const cleanedSelectorBody = selectorBody.replace(/\?\.(?=\w)/g, ".");

          // Create the new selector with loading state handling (single line for better formatting)
          const newSelector = `(${paramName}) => ${paramName}.$isLoaded ? ${cleanedSelectorBody} : ${paramName}.$jazz.loadingState === "loading" ? undefined : null`;

          selectProperty.setInitializer(newSelector);
        }
      } else {
        // Hook WITHOUT existing selector - add one
        // But first check if the parameter name from existing selector should be used
        const newSelector = `(${paramName}) => ${paramName}.$isLoaded ? ${paramName} : ${paramName}.$jazz.loadingState === "loading" ? undefined : null`;

        // Add the select property to the options object
        optionsArg.addPropertyAssignment({
          name: "select",
          initializer: newSelector,
        });
      }
    }
  });
}

/**
 * Migrates if statements that check MaybeLoaded values directly
 * Transforms: if (account) -> if (account.$isLoaded)
 */
function migrateMaybeLoadedIfStatements(sourceFile: SourceFile) {
  function isMaybeLoadedType(type: Type): boolean {
    const properties = type.getProperties();
    const propertyNames = properties.map((p: Symbol) => p.getName());
    return (
      propertyNames.includes("$isLoaded") && propertyNames.includes("$jazz")
    );
  }

  const replacements: Array<{ node: Node; newText: string }> = [];

  // Find if statements that check MaybeLoaded values
  sourceFile.forEachDescendant((node) => {
    if (Node.isIfStatement(node)) {
      const expression = node.getExpression();

      // Check for direct variable reference: if (account)
      if (Node.isIdentifier(expression)) {
        const varName = expression.getText();
        const type = expression.getType();

        if (isMaybeLoadedType(type)) {
          // Replace 'account' with 'account.$isLoaded'
          replacements.push({
            node: expression,
            newText: `${varName}.$isLoaded`,
          });
        }
      }

      // Check for negation: if (!account)
      if (Node.isPrefixUnaryExpression(expression)) {
        const operator = expression.getOperatorToken();
        if (operator === SyntaxKind.ExclamationToken) {
          const operand = expression.getOperand();
          if (Node.isIdentifier(operand)) {
            const varName = operand.getText();
            const type = operand.getType();

            if (isMaybeLoadedType(type)) {
              // Replace 'account' in '!account' with 'account.$isLoaded'
              replacements.push({
                node: operand,
                newText: `${varName}.$isLoaded`,
              });
            }
          }
        }
      }
    }
  });

  // Apply replacements in reverse order to avoid position shifts
  replacements.reverse().forEach(({ node, newText }) => {
    node.replaceWithText(newText);
  });
}

export function runTransform(projectPath: string) {
  let project: Project;

  if (fs.existsSync(`${projectPath}/tsconfig.json`)) {
    project = new Project({
      tsConfigFilePath: `${projectPath}/tsconfig.json`,
      skipFileDependencyResolution: true, // Skip resolving dependencies to save memory
    });
  } else {
    project = new Project({
      skipFileDependencyResolution: true, // Skip resolving dependencies to save memory
    });

    const supportedExtensions = [".ts", ".tsx"];
    const hasSupportedExtension = supportedExtensions.some((ext) =>
      projectPath.endsWith(ext),
    );

    if (hasSupportedExtension) {
      project.addSourceFilesAtPaths(projectPath);
    } else {
      project.addSourceFilesAtPaths([
        `${projectPath}/**/*.{ts,tsx}`,
        `!${projectPath}/**/node_modules/**`,
      ]);
    }
  }

  const sourceFiles = project.getSourceFiles().filter((sourceFile) => {
    return !sourceFile.getFilePath().includes("node_modules");
  });

  let changed = false;

  sourceFiles.forEach((sourceFile) => {
    try {
      console.log(`Transforming: ${sourceFile.getFilePath()}`);
      const originalText = sourceFile.getFullText();
      const transformedText = transformFile(sourceFile);

      if (originalText !== transformedText) {
        sourceFile.saveSync();
        console.log(`Transformed: ${sourceFile.getFilePath()}`);
        changed = true;
      }

      // Free memory by forgetting the source file after processing
      // This helps prevent OOM errors on large projects
      sourceFile.forget();
    } catch (error) {
      console.error(`Error transforming ${sourceFile.getFilePath()}:`, error);
    }
  });

  return changed;
}
