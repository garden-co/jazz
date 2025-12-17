export function isCloudflare() {
  // @ts-ignore
  if (
    typeof navigator !== "undefined" &&
    navigator?.userAgent?.includes("Cloudflare")
  ) {
    return true;
  }

  return false;
}

export const isEvalAllowed = () => {
  if (isCloudflare()) {
    return false;
  }

  try {
    const F = Function;
    new F("");
    return true;
  } catch (_) {
    return false;
  }
};
