import {
  CoID,
  LocalNode,
  RawBinaryCoStream,
  RawCoStream,
  RawCoValue,
} from "cojson";
import { base64URLtoBytes } from "cojson/src/base64url.ts";
import {
  BinaryStreamItem,
  BinaryStreamStart,
  CoStreamItem,
} from "cojson/src/coValues/coStream.ts";
import type { JsonObject, JsonValue } from "cojson/src/jsonValue.ts";
import { useEffect, useState } from "react";
import { PageInfo } from "./types.ts";
import { AccountOrGroupPreview } from "./value-renderer.tsx";

// typeguard for BinaryStreamStart
function isBinaryStreamStart(item: unknown): item is BinaryStreamStart {
  return (
    typeof item === "object" &&
    item !== null &&
    "type" in item &&
    item.type === "start"
  );
}

function detectCoStreamType(value: RawCoStream | RawBinaryCoStream) {
  const firstKey = Object.keys(value.items)[0];
  if (!firstKey)
    return {
      type: "unknown",
    };

  const items = value.items[firstKey as never]?.map((v) => v.value);

  if (!items)
    return {
      type: "unknown",
    };
  const firstItem = items[0];
  if (!firstItem)
    return {
      type: "unknown",
    };
  // This is a binary stream
  if (isBinaryStreamStart(firstItem)) {
    return {
      type: "binary",
      items: items as BinaryStreamItem[],
    };
  } else {
    return {
      type: "coStream",
    };
  }
}

async function getBlobFromCoStream({
  items,
  onlyFirstChunk = false,
}: {
  items: BinaryStreamItem[];
  onlyFirstChunk?: boolean;
}) {
  if (onlyFirstChunk && items.length > 1) {
    items = items.slice(0, 2);
  }

  const chunks: Uint8Array[] = [];

  const binary_U_prefixLength = 8;

  let lastProgressUpdate = Date.now();

  for (const item of items.slice(1)) {
    if (item.type === "end") {
      break;
    }

    if (item.type !== "chunk") {
      console.error("Invalid binary stream chunk", item);
      return undefined;
    }

    const chunk = base64URLtoBytes(item.chunk.slice(binary_U_prefixLength));
    // totalLength += chunk.length;
    chunks.push(chunk);

    if (Date.now() - lastProgressUpdate > 100) {
      lastProgressUpdate = Date.now();
    }
  }
  const defaultMime = "mimeType" in items[0] ? items[0].mimeType : null;

  const blob = new Blob(chunks, defaultMime ? { type: defaultMime } : {});

  const mimeType =
    defaultMime === "" ? await detectPDFMimeType(blob) : defaultMime;

  return {
    blob,
    mimeType: mimeType as string,
    unfinishedChunks: items.length > 1,
    totalSize:
      "totalSizeBytes" in items[0]
        ? (items[0].totalSizeBytes as number)
        : undefined,
  };
}

const detectPDFMimeType = async (blob: Blob): Promise<string> => {
  const arrayBuffer = await blob.slice(0, 4).arrayBuffer();
  const uint8Array = new Uint8Array(arrayBuffer);
  const header = uint8Array.reduce(
    (acc, byte) => acc + String.fromCharCode(byte),
    "",
  );

  if (header === "%PDF") {
    return "application/pdf";
  }
  return "application/octet-stream";
};

const BinaryDownloadButton = ({
  pdfBlob,
  fileName = "document",
  label,
  mimeType,
}: {
  pdfBlob: Blob;
  mimeType?: string;
  fileName?: string;
  label: string;
}) => {
  const downloadFile = () => {
    const url = URL.createObjectURL(
      new Blob([pdfBlob], mimeType ? { type: mimeType } : {}),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download =
      mimeType === "application/pdf" ? `${fileName}.pdf` : fileName;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  return (
    <button onClick={downloadFile}>
      ⬇️ {label}
      {/* Download {mimeType === "application/pdf" ? "PDF" : "File"} */}
    </button>
  );
};

const LabelContentPair = ({
  label,
  content,
}: {
  label: string;
  content: React.ReactNode;
}) => {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.375rem" }}>
      <span>{label}</span>
      <span>{content}</span>
    </div>
  );
};

function RenderCoBinaryStream({
  value,
  items,
}: {
  items: BinaryStreamItem[];
  value: RawBinaryCoStream;
}) {
  const [file, setFile] = useState<
    | {
        blob: Blob;
        mimeType: string;
        unfinishedChunks: boolean;
        totalSize: number | undefined;
      }
    | undefined
    | null
  >(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    // load only the first chunk to get the mime type and size
    getBlobFromCoStream({
      items,
      onlyFirstChunk: true,
    })
      .then((v) => {
        if (v) {
          setFile(v);
          if (v.mimeType.includes("image")) {
            // If it's an image, load the full blob
            getBlobFromCoStream({
              items,
            }).then((s) => {
              if (s) setFile(s);
            });
          }
        }
      })
      .finally(() => setIsLoading(false));
  }, [items]);

  if (!isLoading && !file) return <div>No blob</div>;

  if (isLoading) return <div>Loading...</div>;
  if (!file) return <div>No blob</div>;

  const { blob, mimeType } = file;

  const sizeInKB = (file.totalSize || 0) / 1024;

  return (
    <div
      style={{
        marginTop: "2rem",
        display: "flex",
        flexDirection: "column",
        gap: "2rem",
      }}
    >
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(3, 1fr)",
          gap: "0.5rem",
          maxWidth: "48rem",
        }}
      >
        <LabelContentPair
          label="Mime Type"
          content={
            <span
              style={{
                fontFamily: "monospace",
                backgroundColor: "rgb(243 244 246)",
                borderRadius: "0.25rem",
                padding: "0.25rem 0.5rem",
                fontSize: "0.875rem",
              }}
            >
              {mimeType || "No mime type"}
            </span>
          }
        />
        <LabelContentPair
          label="Size"
          content={<span>{sizeInKB.toFixed(2)} KB</span>}
        />
        <LabelContentPair
          label="Download"
          content={
            <BinaryDownloadButton
              fileName={value.id.toString()}
              pdfBlob={blob}
              mimeType={mimeType}
              label={
                mimeType === "application/pdf"
                  ? "Download PDF"
                  : "Download File"
              }
            />
          }
        />
      </div>
      {mimeType === "image/png" || mimeType === "image/jpeg" ? (
        <LabelContentPair
          label="Preview"
          content={
            <div
              style={{
                backgroundColor: "rgb(249 250 251)",
                padding: "0.75rem",
                borderRadius: "0.125rem",
              }}
            >
              <RenderBlobImage blob={blob} />
            </div>
          }
        />
      ) : null}
    </div>
  );
}

function RenderCoStream({
  value,
  node,
}: {
  value: RawCoStream;
  node: LocalNode;
}) {
  const streamPerUser = Object.keys(value.items);
  const userCoIds = streamPerUser.map((stream) => stream.split("_session")[0]);

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(3, 1fr)",
        gap: "0.5rem",
      }}
    >
      {userCoIds.map((id, idx) => (
        <div
          style={{
            padding: "0.75rem",
            borderRadius: "0.5rem",
            overflow: "hidden",
            backgroundColor: "white",
            border: "1px solid #e5e7eb",
            cursor: "pointer",
            boxShadow: "0 1px 2px 0 rgb(0 0 0 / 0.05)",
            transition: "background-color 0.2s",
          }}
          onMouseOver={(e) =>
            (e.currentTarget.style.backgroundColor =
              "rgba(243, 244, 246, 0.05)")
          }
          onMouseOut={(e) => (e.currentTarget.style.backgroundColor = "white")}
          key={id}
        >
          <AccountOrGroupPreview coId={id as CoID<RawCoValue>} node={node} />
          {/* @ts-expect-error - TODO: fix types */}
          {value.items[streamPerUser[idx]]?.map(
            (item: CoStreamItem<JsonValue>) => (
              <div>
                {new Date(item.madeAt).toLocaleString()}{" "}
                {JSON.stringify(item.value)}
              </div>
            ),
          )}
        </div>
      ))}
    </div>
  );
}

export function CoStreamView({
  value,
  node,
}: {
  data: JsonObject;
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
  value: RawCoStream;
}) {
  // if (!value) return <div>No value</div>;

  const streamType = detectCoStreamType(value);

  if (streamType.type === "binary") {
    if (streamType.items === undefined) {
      return <div>No binary stream</div>;
    }

    return (
      <RenderCoBinaryStream
        value={value as RawBinaryCoStream}
        items={streamType.items}
      />
    );
  }

  if (streamType.type === "coStream") {
    return <RenderCoStream value={value} node={node} />;
  }

  if (streamType.type === "unknown") return <div>Unknown stream type</div>;

  return <div>Unknown stream type</div>;
}

function RenderBlobImage({ blob }: { blob: Blob }) {
  const urlCreator = window.URL || window.webkitURL;
  return <img src={urlCreator.createObjectURL(blob)} />;
}
