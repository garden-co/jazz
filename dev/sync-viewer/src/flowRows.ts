export type FlowAttributeSource = {
  payload?: string;
  payload_json?: string;
  peer_kind?: string;
  peer_id?: string;
  tier?: string;
  fields?: string;
};

export type FlowRow = FlowAttributeSource & {
  Timestamp: string;
  ServiceName: string;
  SpanName: "sync.send" | "sync.recv";
  thread?: string;
};

export type FlowAttrs = {
  payload: string;
  peer_kind: string;
  peer_id: string;
  tier: string;
  payload_json: string;
};

export type FlowPayloadDetail = {
  label: string;
  value: string;
  kind: "text" | "json";
};

export function resolveFlowAttrs(row: FlowAttributeSource): FlowAttrs {
  let payload = row.payload || "";
  let peer_kind = row.peer_kind || "";
  let peer_id = row.peer_id || "";
  let tier = row.tier || "";
  let payload_json = row.payload_json || "";
  if ((!payload || !peer_kind || !payload_json) && row.fields) {
    try {
      const f = JSON.parse(row.fields) as Record<string, string>;
      payload = payload || f.payload || "";
      peer_kind = peer_kind || f.peer_kind || "";
      peer_id = peer_id || f.peer_id || "";
      tier = tier || f.tier || "";
      payload_json = payload_json || f.payload_json || "";
    } catch {
      // ignore malformed legacy field blobs
    }
  }
  return { payload, peer_kind, peer_id, tier, payload_json };
}

export function flowPayloadDetails(attrs: FlowAttrs): FlowPayloadDetail[] {
  const details: FlowPayloadDetail[] = [];
  if (attrs.payload) {
    details.push({ label: "payload", value: attrs.payload, kind: "text" });
  }
  if (attrs.payload_json) {
    details.push({ label: "payload_json", value: attrs.payload_json, kind: "json" });
  }
  return details;
}
