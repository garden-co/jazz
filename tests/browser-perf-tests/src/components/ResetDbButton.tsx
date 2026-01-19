import { useState } from "react";
import { Account, cojsonInternals } from "jazz-tools";

cojsonInternals.setMaxInFlightLoadsPerPeer(1000);

async function deleteDatabase(name: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const request = indexedDB.deleteDatabase(name);

    request.onsuccess = () => resolve();

    request.onerror = () => reject(request.error);

    request.onblocked = () => {
      reject(request.error);
    };
  });
}

export function ResetDbButton() {
  const [resetStatus, setResetStatus] = useState<
    "idle" | "resetting" | "done" | "error"
  >("idle");

  const handleReset = async () => {
    if (resetStatus !== "idle") return;
    setResetStatus("resetting");

    try {
      // Close Jazz connections first
      await Account.getMe().$jazz.localNode.gracefulShutdown();

      const databases = await indexedDB.databases();

      await Promise.all(
        databases.map((db) => {
          if (!db.name) return Promise.resolve();
          return deleteDatabase(db.name);
        }),
      );

      setResetStatus("done");
      window.location.reload();
    } catch (error) {
      console.error("Failed to reset database:", error);
      setResetStatus("error");
    }
  };

  return (
    <button
      onClick={handleReset}
      disabled={resetStatus !== "idle" && resetStatus !== "error"}
      style={{
        padding: "10px 16px",
        background:
          resetStatus === "done"
            ? "#1a3a1a"
            : resetStatus === "error"
              ? "#5a1a1a"
              : "#3a1a1a",
        border: `1px solid ${
          resetStatus === "done"
            ? "#2a5a2a"
            : resetStatus === "error"
              ? "#8a2a2a"
              : "#5a2a2a"
        }`,
        borderRadius: "8px",
        color:
          resetStatus === "done"
            ? "#4ade80"
            : resetStatus === "error"
              ? "#ff4444"
              : "#ff6b6b",
        fontSize: "0.75rem",
        fontWeight: "500",
        cursor:
          resetStatus === "idle" || resetStatus === "error"
            ? "pointer"
            : "default",
        whiteSpace: "nowrap",
      }}
    >
      {resetStatus === "idle" && "Reset DB"}
      {resetStatus === "resetting" && "Resetting..."}
      {resetStatus === "done" && "Reset Done"}
      {resetStatus === "error" && "Retry Reset"}
    </button>
  );
}
