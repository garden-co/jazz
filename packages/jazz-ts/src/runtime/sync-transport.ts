/**
 * Shared sync transport utilities.
 *
 * Used by both `client.ts` (main thread) and `groove-worker.ts` (worker)
 * to avoid duplicating binary frame parsing, sync POST logic, and
 * catalogue payload detection.
 */

/** Auth and identity context for sync operations. */
export interface SyncAuth {
  jwtToken?: string;
  adminSecret?: string;
  clientId?: string;
}

/** Callbacks for stream events. */
export interface StreamCallbacks {
  onSyncMessage(payloadJson: string): void;
  onConnected?(clientId: string): void;
}

/**
 * Check if a sync payload is for a catalogue object (schema or lens).
 * Catalogue payloads use admin-secret auth instead of JWT.
 */
export function isCataloguePayload(payload: any): boolean {
  const metadata = payload?.ObjectUpdated?.metadata?.metadata;
  if (metadata) {
    const t = metadata["type"];
    return t === "catalogue_schema" || t === "catalogue_lens";
  }
  return false;
}

/**
 * POST a sync payload to the server.
 *
 * Catalogue payloads get the admin-secret header; everything else gets JWT.
 */
export async function sendSyncPayload(
  serverUrl: string,
  payload: any,
  auth: SyncAuth,
  logPrefix = "",
): Promise<void> {
  try {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };

    if (isCataloguePayload(payload)) {
      if (auth.adminSecret) {
        headers["X-Jazz-Admin-Secret"] = auth.adminSecret;
      }
    } else if (auth.jwtToken) {
      headers["Authorization"] = `Bearer ${auth.jwtToken}`;
    }

    const body = JSON.stringify({
      payload,
      client_id: auth.clientId ?? "00000000-0000-0000-0000-000000000000",
    });

    const response = await fetch(`${serverUrl}/sync`, {
      method: "POST",
      headers,
      body,
    });

    if (!response.ok) {
      console.error(`${logPrefix}Sync POST error:`, response.statusText);
    }
  } catch (e) {
    console.error(`${logPrefix}Sync POST error:`, e);
  }
}

/**
 * Read length-prefixed binary frames from a ReadableStreamDefaultReader.
 *
 * Each frame is: 4-byte big-endian length + UTF-8 JSON payload.
 * Calls `callbacks.onSyncMessage` for SyncUpdate events and
 * `callbacks.onConnected` for Connected events.
 *
 * Returns when the stream ends or is aborted.
 */
export async function readBinaryFrames(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  callbacks: StreamCallbacks,
  logPrefix = "",
): Promise<void> {
  let buffer = new Uint8Array(0);

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    // Append chunk to buffer
    const newBuffer = new Uint8Array(buffer.length + value.length);
    newBuffer.set(buffer);
    newBuffer.set(value, buffer.length);
    buffer = newBuffer;

    // Read complete frames
    while (buffer.length >= 4) {
      const len = new DataView(buffer.buffer, buffer.byteOffset).getUint32(0, false);
      if (buffer.length < 4 + len) break;
      const json = new TextDecoder().decode(buffer.slice(4, 4 + len));
      buffer = buffer.slice(4 + len);
      try {
        const event = JSON.parse(json);
        if (event.type === "Connected" && event.client_id) {
          callbacks.onConnected?.(event.client_id);
        } else if (event.type === "SyncUpdate") {
          callbacks.onSyncMessage(JSON.stringify(event.payload));
        }
      } catch (e) {
        console.error(`${logPrefix}Stream parse error:`, e);
      }
    }
  }
}
