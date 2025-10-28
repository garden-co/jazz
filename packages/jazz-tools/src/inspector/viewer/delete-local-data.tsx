import { Button } from "../ui/button.js";
import { Modal } from "../ui/modal.js";
import { Input } from "../ui/input.js";
import { useState } from "react";

const DELETE_LOCAL_DATA_STRING = "delete my local data";

export function DeleteLocalData() {
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [confirmDeleteString, setConfirmDeleteString] = useState("");

  return (
    <>
      <Button variant="destructive" onClick={() => setShowDeleteModal(true)}>
        Delete my local data
      </Button>
      <Modal
        isOpen={showDeleteModal}
        onClose={() => setShowDeleteModal(false)}
        heading="Delete Local Data"
        showButtons={false}
      >
        <div
          style={{
            margin: "0 0 1rem 0",
            color: "var(--j-text-color)",
            display: "flex",
            flexDirection: "column",
            gap: "0.5rem",
          }}
        >
          <p>
            This action <strong>cannot</strong> be undone.
          </p>
          <p>
            Be aware that the following data will be{" "}
            <strong>permanently</strong> deleted:
          </p>
          <ul style={{ listStyleType: "disc", paddingLeft: "1rem" }}>
            <li>
              Unsynced data for <strong>all apps</strong> on{" "}
              <code>{window.location.origin}</code>
            </li>
            <li>Accounts</li>
            <li>Logged in sessions</li>
          </ul>
          <p></p>
        </div>
        <Input
          label={`Type "${DELETE_LOCAL_DATA_STRING}" to confirm`}
          placeholder={DELETE_LOCAL_DATA_STRING}
          value={confirmDeleteString}
          onChange={(e) => {
            setConfirmDeleteString(e.target.value);
          }}
        />
        <p
          style={{
            margin: "0 0 1rem 0",
            color: "var(--j-text-color)",
            display: "flex",
            flexDirection: "column",
            gap: "0.5rem",
          }}
        >
          <small>
            Data synced to a sync server will <strong>not</strong> be deleted,
            and will be synced when you log in again.
          </small>
        </p>
        <div
          style={{
            display: "flex",
            marginTop: "0.5rem",
            justifyContent: "flex-end",
            gap: "0.5rem",
          }}
        >
          <Button variant="secondary" onClick={() => setShowDeleteModal(false)}>
            Cancel
          </Button>
          <Button
            variant="destructive"
            disabled={confirmDeleteString !== DELETE_LOCAL_DATA_STRING}
            onClick={() => {
              const jazzKeys = Object.keys(localStorage).filter(
                (key) => key.startsWith("jazz-") || key.startsWith("co_z"),
              );
              jazzKeys.forEach((key) => localStorage.removeItem(key));
              indexedDB.deleteDatabase("jazz-storage");
              window.location.reload();
              setShowDeleteModal(false);
            }}
          >
            I'm sure, delete my local data
          </Button>
        </div>
      </Modal>
    </>
  );
}
