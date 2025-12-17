export function isCloudflare() {
  if (
    // @ts-ignore
    typeof navigator !== "undefined" &&
    // @ts-ignore
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
