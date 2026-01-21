export function formatTime(startTime: number): string {
  const date = new Date(performance.timeOrigin + startTime);
  return date.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  });
}

export function formatDuration(duration: number): string {
  if (duration < 1) {
    return `${(duration * 1000).toFixed(0)}μs`;
  }
  if (duration < 1000) {
    return `${duration.toFixed(2)}ms`;
  }
  return `${(duration / 1000).toFixed(2)}s`;
}

export function getCallerLocation(
  stack: string | undefined,
): string | undefined {
  if (!stack) return undefined;

  const lines = stack.split("\n").slice(2, 15);

  const normalizeLine = (line: string) =>
    line.replace(/https?:\/\/[^/\s)]+/g, "");

  const userFrame = lines.find(
    (line) =>
      !line.includes("node_modules") &&
      !line.includes("useCoValueSubscription") &&
      !line.includes("useCoState") &&
      !line.includes("useAccount") &&
      !line.includes("useSuspenseCoState") &&
      !line.includes("useSuspenseAccount") &&
      !line.includes("jazz-tools") &&
      !line.includes("trackLoadingPerformance"),
  );

  if (userFrame) {
    const cleanedFrame = normalizeLine(userFrame).trim();
    const match = cleanedFrame.match(/\(?([^)]+:\d+:\d+)\)?$/);
    if (match) {
      return match[1];
    }
    return cleanedFrame;
  }

  return lines[0] ? normalizeLine(lines[0]).trim() : undefined;
}

export function getCallerStack(stack: string | undefined): string | undefined {
  if (!stack) return undefined;

  const lines = stack.split("\n").slice(2, 15);

  return lines
    .filter(
      (line) =>
        !line.includes("Error:") &&
        !line.includes("renderWithHooks") &&
        !line.includes("react-stack-bottom-frame"),
    )
    .map((line) => line.replace(/https?:\/\/[^/\s)]+/g, "").trim())
    .reverse()
    .join("\n");
}
