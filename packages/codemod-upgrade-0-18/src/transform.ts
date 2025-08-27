import {
  Project,
  SyntaxKind,
  Node,
  PropertyAccessExpression,
  CallExpression,
  BinaryExpression,
  ElementAccessExpression,
  DeleteExpression,
  SourceFile,
  VariableStatement,
  VariableDeclaration,
  ObjectBindingPattern,
} from "ts-morph";
import fs from "node:fs";

// Helper function to check if a property access has optional chaining
function hasOptionalChaining(node: PropertyAccessExpression): boolean {
  return node.hasQuestionDotToken();
}

// Helper function to check if an element access has optional chaining
function hasOptionalChainingElement(node: ElementAccessExpression): boolean {
  return node.hasQuestionDotToken();
}

// Helper function to check if any node in a chain has optional chaining
function hasOptionalChainingInChain(node: Node): boolean {
  if (node.getKind() === SyntaxKind.PropertyAccessExpression) {
    const propAccess = node as PropertyAccessExpression;
    return (
      hasOptionalChaining(propAccess) ||
      hasOptionalChainingInChain(propAccess.getExpression())
    );
  } else if (node.getKind() === SyntaxKind.ElementAccessExpression) {
    const elementAccess = node as ElementAccessExpression;
    return (
      hasOptionalChainingElement(elementAccess) ||
      hasOptionalChainingInChain(elementAccess.getExpression())
    );
  }
  return false;
}

// Helper function to get the text with optional chaining preserved
function getTextWithOptionalChaining(node: Node): string {
  if (node.getKind() === SyntaxKind.PropertyAccessExpression) {
    const propAccess = node as PropertyAccessExpression;
    const expressionText = getTextWithOptionalChaining(
      propAccess.getExpression(),
    );
    if (hasOptionalChaining(propAccess)) {
      return `${expressionText}?.${propAccess.getNameNode().getText()}`;
    }
    return `${expressionText}.${propAccess.getNameNode().getText()}`;
  } else if (node.getKind() === SyntaxKind.ElementAccessExpression) {
    const elementAccess = node as ElementAccessExpression;
    const expressionText = getTextWithOptionalChaining(
      elementAccess.getExpression(),
    );
    const argument = elementAccess.getArgumentExpression();
    if (argument) {
      return `${expressionText}[${argument.getText()}]`;
    }
    return expressionText;
  }
  return node.getText();
}

// Helper function to create property access text with optional chaining preserved
function createPropertyAccessText(
  object: Node,
  propertyName: string,
  useOptionalChaining: boolean = false,
): string {
  const objectText = getTextWithOptionalChaining(object);
  if (useOptionalChaining) {
    // If the property name contains dots, only apply optional chaining to the first part
    if (propertyName.includes(".")) {
      const [firstPart, ...rest] = propertyName.split(".");
      return `${objectText}?.${firstPart}.${rest.join(".")}`;
    }
    return `${objectText}?.${propertyName}`;
  }
  return `${objectText}.${propertyName}`;
}

export function transformFile(sourceFile: SourceFile): string {
  // 1. Transform property access patterns
  transformPropertyAccess(sourceFile);

  // 2. Transform destructuring assignments
  transformDestructuringAssignments(sourceFile);

  // 3. Transform _refs.property to $jazz.refs.property
  transformRefsAccess(sourceFile);

  // 4. Transform _edits.property to $jazz.getEdits().property
  transformEditsAccess(sourceFile);

  // 5. Transform method calls
  transformMethodCalls(sourceFile);

  // 6. Transform array operations
  transformArrayOperations(sourceFile);

  // 7. Transform property assignments
  transformPropertyAssignments(sourceFile);

  // 8. Transform array index assignments
  transformArrayIndexAssignments(sourceFile);

  // 9. Transform delete statements
  transformDeleteStatements(sourceFile);

  // 10. Transform splice operations
  transformSpliceOperations(sourceFile);

  // 11. Remove .castAs() calls
  removeCastAsCalls(sourceFile);

  return sourceFile.getFullText();
}

function isJazzValue(object: Node) {
  // When transformed to text the import path is shown
  // Then we check that this is something that comes from jazz-tools
  return (
    object.getType().getText().includes("jazz-tools") ||
    object.getType().isAny()
  );
}

function transformPropertyAccess(sourceFile: SourceFile) {
  // Transform obj.id to obj.$jazz.id
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.PropertyAccessExpression) {
      const propertyAccess = node as PropertyAccessExpression;
      const propertyName = propertyAccess.getNameNode();

      if (node.getType().isString() || node.getType().isNumber()) {
        return;
      }

      if (propertyName.getText() === "id") {
        const object = propertyAccess.getExpression();

        if (
          !isJazzValue(object) ||
          object.getType().getText().includes(".Ref<")
        ) {
          return;
        }

        if (object.getText().includes(".$jazz")) {
          return;
        }

        if (object.getKind() === SyntaxKind.PropertyAccessExpression) {
          const objProperty = (
            object as PropertyAccessExpression
          ).getNameNode();
          if (objProperty.getText() === "$jazz") {
            return; // Already transformed
          }
        }

        // Preserve optional chaining
        const hasOptional = hasOptionalChaining(propertyAccess);
        const newText = createPropertyAccessText(
          object,
          "$jazz.id",
          hasOptional,
        );
        propertyAccess.replaceWithText(newText);
      }
    }
  });

  // Transform obj._owner to obj.$jazz.owner
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.PropertyAccessExpression) {
      const propertyAccess = node as PropertyAccessExpression;
      const propertyName = propertyAccess.getNameNode();

      if (propertyName.getText() === "_owner") {
        const object = propertyAccess.getExpression();
        const hasOptional = hasOptionalChaining(propertyAccess);
        let newText = createPropertyAccessText(
          object,
          "$jazz.owner",
          hasOptional,
        );

        propertyAccess.replaceWithText(newText);
      }
    }
  });

  // Transform obj._type to obj.$type$
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.PropertyAccessExpression) {
      const propertyAccess = node as PropertyAccessExpression;
      const propertyName = propertyAccess.getNameNode();

      if (propertyName.getText() === "_type") {
        const object = propertyAccess.getExpression();
        const hasOptional = hasOptionalChaining(propertyAccess);
        const newText = createPropertyAccessText(object, "$type$", hasOptional);
        propertyAccess.replaceWithText(newText);
      }
    }
  });

  // Transform obj._createdAt to obj.$jazz.createdAt
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.PropertyAccessExpression) {
      const propertyAccess = node as PropertyAccessExpression;
      const propertyName = propertyAccess.getNameNode();

      if (propertyName.getText() === "_createdAt") {
        const object = propertyAccess.getExpression();
        const hasOptional = hasOptionalChaining(propertyAccess);
        const newText = createPropertyAccessText(
          object,
          "$jazz.createdAt",
          hasOptional,
        );
        propertyAccess.replaceWithText(newText);
      } else if (propertyName.getText() === "_lastUpdatedAt") {
        const object = propertyAccess.getExpression();
        const hasOptional = hasOptionalChaining(propertyAccess);
        const newText = createPropertyAccessText(
          object,
          "$jazz.lastUpdatedAt",
          hasOptional,
        );
        propertyAccess.replaceWithText(newText);
      }
    }
  });
}

function transformDestructuringAssignments(sourceFile: SourceFile) {
  // Transform destructuring assignments like const { id } = coValue to const { id } = coValue.$jazz
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.VariableStatement) {
      const variableStatement = node as VariableStatement;
      const declarationList = variableStatement.getDeclarationList();

      declarationList.getDeclarations().forEach((declaration) => {
        const nameNode = declaration.getNameNode();
        const initializer = declaration.getInitializer();

        // Check if this is an object destructuring pattern
        if (
          nameNode.getKind() === SyntaxKind.ObjectBindingPattern &&
          initializer
        ) {
          const objectPattern = nameNode as ObjectBindingPattern;

          // Check if any of the destructured properties are Jazz properties we need to transform
          const hasJazzProperties = objectPattern
            .getElements()
            .some((element) => {
              const propertyName = element.getPropertyNameNode();
              if (propertyName) {
                const name = propertyName.getText();
                return (
                  name === "id" ||
                  name === "_owner" ||
                  name === "_type" ||
                  name === "_createdAt" ||
                  name === "_lastUpdatedAt"
                );
              }
              return false;
            });

          if (hasJazzProperties && isJazzValue(initializer)) {
            // Check if initializer already includes $jazz
            if (initializer.getText().includes(".$jazz")) {
              return;
            }

            // Transform based on the properties being destructured
            const elements = objectPattern.getElements();
            const needsJazzTransform = elements.some((element) => {
              const propertyName = element.getPropertyNameNode();
              if (propertyName) {
                const name = propertyName.getText();
                return (
                  name === "id" ||
                  name === "_owner" ||
                  name === "_createdAt" ||
                  name === "_lastUpdatedAt"
                );
              }
              return false;
            });

            const needsTypeTransform = elements.some((element) => {
              const propertyName = element.getPropertyNameNode();
              if (propertyName) {
                const name = propertyName.getText();
                return name === "_type";
              }
              return false;
            });

            if (needsJazzTransform && needsTypeTransform) {
              // Mixed case: split into multiple statements
              const jazzElements = elements.filter((element) => {
                const propertyName = element.getPropertyNameNode();
                if (propertyName) {
                  const name = propertyName.getText();
                  return (
                    name === "id" ||
                    name === "_owner" ||
                    name === "_createdAt" ||
                    name === "_lastUpdatedAt"
                  );
                }
                return false;
              });

              const typeElements = elements.filter((element) => {
                const propertyName = element.getPropertyNameNode();
                if (propertyName) {
                  const name = propertyName.getText();
                  return name === "_type";
                }
                return false;
              });

              const otherElements = elements.filter((element) => {
                const propertyName = element.getPropertyNameNode();
                if (propertyName) {
                  const name = propertyName.getText();
                  return ![
                    "id",
                    "_owner",
                    "_type",
                    "_createdAt",
                    "_lastUpdatedAt",
                  ].includes(name);
                }
                return true;
              });

              // Build new variable statements
              let newStatements: string[] = [];

              if (jazzElements.length > 0) {
                const jazzElementsText = jazzElements
                  .map((el) => {
                    const propertyName = el.getPropertyNameNode();
                    const name = el.getName();
                    if (propertyName && name) {
                      const propText = propertyName.getText();
                      if (propText === "_owner") {
                        return name !== "owner" ? `owner: ${name}` : "owner";
                      } else if (propText === "_createdAt") {
                        return name !== "createdAt"
                          ? `createdAt: ${name}`
                          : "createdAt";
                      } else if (propText === "_lastUpdatedAt") {
                        return name !== "lastUpdatedAt"
                          ? `lastUpdatedAt: ${name}`
                          : "lastUpdatedAt";
                      }
                      return el.getText();
                    }
                    return el.getText();
                  })
                  .join(", ");
                newStatements.push(
                  `const { ${jazzElementsText} } = ${initializer.getText()}.$jazz`,
                );
              }

              if (typeElements.length > 0) {
                const typeElementsText = typeElements
                  .map((el) => {
                    const name = el.getName();
                    return name !== "$type$" ? `$type$: ${name}` : "$type$";
                  })
                  .join(", ");
                newStatements.push(
                  `const { ${typeElementsText} } = ${initializer.getText()}`,
                );
              }

              if (otherElements.length > 0) {
                const otherElementsText = otherElements
                  .map((el) => el.getText())
                  .join(", ");
                newStatements.push(
                  `const { ${otherElementsText} } = ${initializer.getText()}`,
                );
              }

              variableStatement.replaceWithText(
                newStatements.join(";\n") + ";",
              );
            } else if (needsJazzTransform) {
              // Transform property names for $jazz access
              const transformedElements = elements
                .map((element) => {
                  const propertyName = element.getPropertyNameNode();
                  const name = element.getName();
                  if (propertyName && name) {
                    const propText = propertyName.getText();
                    if (propText === "_owner") {
                      return name !== "owner" ? `owner: ${name}` : "owner";
                    } else if (propText === "_createdAt") {
                      return name !== "createdAt"
                        ? `createdAt: ${name}`
                        : "createdAt";
                    } else if (propText === "_lastUpdatedAt") {
                      return name !== "lastUpdatedAt"
                        ? `lastUpdatedAt: ${name}`
                        : "lastUpdatedAt";
                    }
                    return element.getText();
                  }
                  return element.getText();
                })
                .join(", ");

              const newText = `const { ${transformedElements} } = ${initializer.getText()}.$jazz`;
              variableStatement.replaceWithText(newText);
            } else if (needsTypeTransform) {
              // Transform _type to $type$
              const transformedElements = elements
                .map((element) => {
                  const propertyName = element.getPropertyNameNode();
                  const name = element.getName();
                  if (propertyName && name) {
                    const propText = propertyName.getText();
                    if (propText === "_type") {
                      return name !== "$type$" ? `$type$: ${name}` : "$type$";
                    }
                    return element.getText();
                  }
                  return element.getText();
                })
                .join(", ");

              const newText = `const { ${transformedElements} } = ${initializer.getText()}`;
              variableStatement.replaceWithText(newText);
            }
          }
        }
      });
    }
  });
}

function transformRefsAccess(sourceFile: SourceFile) {
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.PropertyAccessExpression) {
      const propertyAccess = node as PropertyAccessExpression;
      const object = propertyAccess.getExpression();

      if (object.getKind() === SyntaxKind.PropertyAccessExpression) {
        const objProperty = (object as PropertyAccessExpression).getNameNode();
        if (objProperty.getText() === "_refs") {
          const baseObject = (
            object as PropertyAccessExpression
          ).getExpression();
          const property = propertyAccess.getNameNode();
          const hasOptional = hasOptionalChaining(propertyAccess);
          const newText = createPropertyAccessText(
            baseObject,
            `$jazz.refs.${property.getText()}`,
            hasOptional,
          );
          propertyAccess.replaceWithText(newText);
        }
      }
    }
  });
}

function transformEditsAccess(sourceFile: SourceFile) {
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.PropertyAccessExpression) {
      const propertyAccess = node as PropertyAccessExpression;
      const object = propertyAccess.getExpression();

      if (object.getKind() === SyntaxKind.PropertyAccessExpression) {
        const objProperty = (object as PropertyAccessExpression).getNameNode();
        if (objProperty.getText() === "_edits") {
          const baseObject = (
            object as PropertyAccessExpression
          ).getExpression();
          const property = propertyAccess.getNameNode();
          const hasOptional = hasOptionalChaining(propertyAccess);
          const newText = createPropertyAccessText(
            baseObject,
            `$jazz.getEdits().${property.getText()}`,
            hasOptional,
          );
          propertyAccess.replaceWithText(newText);
        }
      }
    }
  });
}

function transformMethodCalls(sourceFile: SourceFile) {
  const methodNames = [
    "waitForSync",
    "waitForAllCoValuesSync",
    "subscribe",
    "ensureLoaded",
    "applyDiff",
  ];

  methodNames.forEach((methodName) => {
    sourceFile.forEachDescendant((node) => {
      if (node.getKind() === SyntaxKind.CallExpression) {
        const callExpression = node as CallExpression;
        const callee = callExpression.getExpression();

        if (callee.getKind() === SyntaxKind.PropertyAccessExpression) {
          const property = (callee as PropertyAccessExpression).getNameNode();

          if (property.getText() === methodName) {
            const baseObject = (
              callee as PropertyAccessExpression
            ).getExpression();

            if (baseObject.getText().includes(".$jazz")) {
              return;
            }

            const args = callExpression
              .getArguments()
              .map((arg) => arg.getText())
              .join(", ");

            // Preserve optional chaining from the method call
            const hasOptional = hasOptionalChaining(
              callee as PropertyAccessExpression,
            );
            const newText = createPropertyAccessText(
              baseObject,
              `$jazz.${methodName}(${args})`,
              hasOptional,
            );
            callExpression.replaceWithText(newText);
          }
        }
      }
    });
  });
}

function transformArrayOperations(sourceFile: SourceFile) {
  const arrayOperationNames = ["push", "unshift", "shift", "pop"];

  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.CallExpression) {
      const callExpression = node as CallExpression;
      const callee = callExpression.getExpression();

      if (callee.getKind() === SyntaxKind.PropertyAccessExpression) {
        const property = (callee as PropertyAccessExpression).getNameNode();
        const propertyName = property.getText();

        if (arrayOperationNames.includes(propertyName)) {
          const baseObject = (
            callee as PropertyAccessExpression
          ).getExpression();

          if (!isJazzValue(baseObject)) {
            return;
          }

          if (baseObject.getText().includes(".$jazz")) {
            return;
          }

          const args = callExpression
            .getArguments()
            .map((arg) => arg.getText())
            .join(", ");

          // Preserve optional chaining from the method call
          const hasOptional = hasOptionalChaining(
            callee as PropertyAccessExpression,
          );
          const newText = createPropertyAccessText(
            baseObject,
            `$jazz.${propertyName}(${args})`,
            hasOptional,
          );
          callExpression.replaceWithText(newText);
        }
      }
    }
  });
}

function transformPropertyAssignments(sourceFile: SourceFile) {
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.BinaryExpression) {
      const binaryExpression = node as BinaryExpression;
      if (binaryExpression.getOperatorToken().getText() === "=") {
        const left = binaryExpression.getLeft();
        const right = binaryExpression.getRight();

        if (left.getKind() === SyntaxKind.PropertyAccessExpression) {
          const leftPropertyAccess = left as PropertyAccessExpression;

          // Skip if it's already a $jazz.set call
          const leftObject = leftPropertyAccess.getExpression();

          if (!isJazzValue(leftObject)) {
            return;
          }

          if (leftObject.getKind() === SyntaxKind.PropertyAccessExpression) {
            const objProperty = (
              leftObject as PropertyAccessExpression
            ).getNameNode();
            if (objProperty.getText() === "$jazz") {
              return;
            }
          }

          const propertyName = leftPropertyAccess.getNameNode().getText();
          const object = leftPropertyAccess.getExpression();

          // Handle undefined assignments as delete
          if (
            right.getKind() === SyntaxKind.Identifier &&
            right.getText() === "undefined"
          ) {
            const hasOptional = hasOptionalChaining(leftPropertyAccess);
            const newText = createPropertyAccessText(
              object,
              `$jazz.delete("${propertyName}")`,
              hasOptional,
            );
            binaryExpression.replaceWithText(newText);
          } else {
            const leftObject = leftPropertyAccess.getExpression();
            const hasOptional = hasOptionalChaining(leftPropertyAccess);
            if (leftObject.getType().isUnion()) {
              const newText = createPropertyAccessText(
                object,
                `$jazz.applyDiff({"${propertyName}": ${right.getText()}})`,
                hasOptional,
              );
              binaryExpression.replaceWithText(newText);
            } else {
              // Handle regular assignments as set
              const newText = createPropertyAccessText(
                object,
                `$jazz.set("${propertyName}", ${right.getText()})`,
                hasOptional,
              );
              binaryExpression.replaceWithText(newText);
            }
          }
        }
      }
    }
  });
}

function transformArrayIndexAssignments(sourceFile: SourceFile) {
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.BinaryExpression) {
      const binaryExpression = node as BinaryExpression;
      if (binaryExpression.getOperatorToken().getText() === "=") {
        const left = binaryExpression.getLeft();
        const right = binaryExpression.getRight();

        if (left.getKind() === SyntaxKind.ElementAccessExpression) {
          const elementAccess = left as ElementAccessExpression;
          const object = elementAccess.getExpression();
          const index = elementAccess.getArgumentExpression();

          if (!isJazzValue(object)) {
            return;
          }

          if (index) {
            // For array index assignments, we need to check if the parent property access has optional chaining
            // Since this is an element access, we'll check the object for optional chaining
            const hasOptional = hasOptionalChainingInChain(object);
            const newText = createPropertyAccessText(
              object,
              `$jazz.set(${index.getText()}, ${right.getText()})`,
              hasOptional,
            );
            binaryExpression.replaceWithText(newText);
          }
        }
      }
    }
  });
}

function transformDeleteStatements(sourceFile: SourceFile) {
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.DeleteExpression) {
      const deleteExpression = node as DeleteExpression;
      const argument = deleteExpression.getExpression();

      if (argument.getKind() === SyntaxKind.PropertyAccessExpression) {
        const property = (argument as PropertyAccessExpression).getNameNode();
        const object = (argument as PropertyAccessExpression).getExpression();

        if (!isJazzValue(object)) {
          return;
        }

        const hasOptional = hasOptionalChaining(
          argument as PropertyAccessExpression,
        );
        const newText = createPropertyAccessText(
          object,
          `$jazz.delete("${property.getText()}")`,
          hasOptional,
        );
        deleteExpression.replaceWithText(newText);
      }
    }
  });
}

function transformSpliceOperations(sourceFile: SourceFile) {
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.CallExpression) {
      const callExpression = node as CallExpression;
      const callee = callExpression.getExpression();

      if (callee.getKind() === SyntaxKind.PropertyAccessExpression) {
        const property = (callee as PropertyAccessExpression).getNameNode();
        if (property.getText() === "splice") {
          const baseObject = (
            callee as PropertyAccessExpression
          ).getExpression();

          if (!isJazzValue(baseObject)) {
            return;
          }

          if (baseObject.getText().includes(".$jazz")) {
            return;
          }

          const args = callExpression
            .getArguments()
            .map((arg) => arg.getText())
            .join(", ");

          // Preserve optional chaining from the method call
          const hasOptional = hasOptionalChaining(
            callee as PropertyAccessExpression,
          );
          const newText = createPropertyAccessText(
            baseObject,
            `$jazz.splice(${args})`,
            hasOptional,
          );
          callExpression.replaceWithText(newText);
        }
      }
    }
  });
}

function removeCastAsCalls(sourceFile: SourceFile) {
  sourceFile.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.CallExpression) {
      const callExpression = node as CallExpression;
      const callee = callExpression.getExpression();

      if (!isJazzValue(callee)) {
        return;
      }

      if (callee.getKind() === SyntaxKind.PropertyAccessExpression) {
        const property = (callee as PropertyAccessExpression).getNameNode();
        if (property.getText() === "castAs") {
          const baseObject = (
            callee as PropertyAccessExpression
          ).getExpression();

          // Replace the entire call expression with just the base object
          // This removes .castAs(Group) and keeps just the object
          callExpression.replaceWithText(baseObject.getText());
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

    // Check if projectPath ends with a supported file extension
    const supportedExtensions = [".ts", ".tsx"];
    const hasSupportedExtension = supportedExtensions.some((ext) =>
      projectPath.endsWith(ext),
    );

    if (hasSupportedExtension) {
      // If it's a specific file, add it directly
      project.addSourceFilesAtPaths(projectPath);
    } else {
      // If it's a directory, add with glob pattern
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
