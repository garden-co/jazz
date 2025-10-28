import { AuthSecretStorage } from "jazz-tools";

async function exportAccountToInspector() {
  const authSecretStorage = new AuthSecretStorage();
  const localStorageData = await authSecretStorage.get();

  if (!localStorageData) {
    console.error("No account data found in localStorage");
    return;
  }

  const encodedAccountSecret = btoa(localStorageData.accountSecret);
  window.open(
    new URL(
      `#/import/${localStorageData?.accountID}/${encodedAccountSecret}`,
      "https://inspector.jazz.tools",
    ).toString(),
    "_blank",
  );
}

function listenForCmdJ() {
  if (typeof window === "undefined") return;

  const cb = (e: any) => {
    if (e.metaKey && e.key === "j") {
      if (
        confirm(
          "Are you sure you want to inspect your account using inspector.jazz.tools? This lets anyone with the secret inspector URL read your data and impersonate you.",
        )
      ) {
        exportAccountToInspector();
      }
    }
  };

  window.addEventListener("keydown", cb);

  return () => {
    window.removeEventListener("keydown", cb);
  };
}

/**
 * Automatically sets up the Cmd+J listener if 'allowJazzInspector' is present in the URL
 * @returns A cleanup function if the listener was set up, undefined otherwise
 */
export function setupInspector() {
  if (typeof window === "undefined") return;

  const url = new URL(window.location.href);
  if (
    url.hash.includes("allowJazzInspector") ||
    process.env.NODE_ENV === "development"
  ) {
    return listenForCmdJ();
  }
}
