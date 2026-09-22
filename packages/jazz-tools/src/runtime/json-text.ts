export function toJsonText(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }

  let encoded: string | undefined;
  try {
    encoded = JSON.stringify(value);
  } catch (error) {
    throw new Error(
      `JSON values must be serializable: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  if (encoded === undefined) {
    throw new Error("JSON values must be serializable");
  }

  return encoded;
}
