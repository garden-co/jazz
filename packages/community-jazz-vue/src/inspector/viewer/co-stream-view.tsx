import {
  CoID,
  LocalNode,
  RawBinaryCoStream,
  RawCoStream,
  RawCoValue,
} from "cojson";
import { base64URLtoBytes } from "cojson";
import { BinaryStreamItem, BinaryStreamStart, CoStreamItem } from "cojson";
import type { JsonObject, JsonValue } from "cojson";
import { styled } from "goober";
import { type PropType, defineComponent, onMounted, ref } from "vue";
import { Badge } from "../ui/badge.js";
import { Button } from "../ui/button.js";
import { AccountOrGroupText } from "./account-or-group-text.js";
import { PageInfo } from "./types.js";

// typeguard for BinaryStreamStart
function isBinaryStreamStart(item: unknown): item is BinaryStreamStart {
  return (
    typeof item === "object" &&
    item !== null &&
    "type" in item &&
    item.type === "start"
  );
}

export function detectCoStreamType(value: RawCoStream | RawBinaryCoStream) {
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
  const defaultMime =
    items[0] && "mimeType" in items[0] ? items[0].mimeType : null;

  const blob = new Blob(chunks, defaultMime ? { type: defaultMime } : {});

  const mimeType =
    defaultMime === "" ? await detectPDFMimeType(blob) : defaultMime;

  return {
    blob,
    mimeType: mimeType as string,
    unfinishedChunks: items.length > 1,
    totalSize:
      items[0] && "totalSizeBytes" in items[0]
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
  return "unknown";
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
    <Button variant="secondary" onClick={downloadFile}>
      ⬇️ {label}
      {/* Download {mimeType === "application/pdf" ? "PDF" : "File"} */}
    </Button>
  );
};

const LabelContentPairContainer = styled("div")`
  display: flex;
  flex-direction: column;
  gap: 0.375rem;
`;

const BinaryStreamGrid = styled("div")`
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 0.5rem;
  max-width: 48rem;
`;

const ImagePreviewContainer = styled("div")`
  background-color: rgb(249 250 251);
  padding: 0.75rem;
  border-radius: var(--j-radius-md);
  @media (prefers-color-scheme: dark) {
    background-color: rgb(28 25 23);
  }
`;

const CoStreamGrid = styled("div")`
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 0.5rem;
`;

const CoStreamItemContainer = styled("div")`
  padding: 0.75rem;
  border-radius: var(--j-radius-lg);
  overflow: hidden;
  border: 1px solid rgb(229 231 235);
  cursor: pointer;
  box-shadow: var(--j-shadow-sm);
  &:hover {
    background-color: rgb(243 244 246 / 0.05);
  }
`;

const LabelContentPair = ({
  label,
  content,
}: {
  label: string;
  content: any;
}) => {
  return (
    <LabelContentPairContainer>
      <span>{label}</span>
      <span>{content}</span>
    </LabelContentPairContainer>
  );
};

const RenderCoBinaryStream = defineComponent({
  name: "RenderCoBinaryStream",
  props: {
    items: {
      type: Array as PropType<BinaryStreamItem[]>,
      required: true,
    },
    value: {
      type: Object as PropType<RawBinaryCoStream>,
      required: true,
    },
  },
  setup(props) {
    const file = ref<
      | {
          blob: Blob;
          mimeType: string;
          unfinishedChunks: boolean;
          totalSize: number | undefined;
        }
      | undefined
      | null
    >(null);
    const isLoading = ref(true);

    onMounted(() => {
      // load only the first chunk to get the mime type and size
      getBlobFromCoStream({
        items: props.items,
        onlyFirstChunk: true,
      })
        .then((v) => {
          if (v) {
            file.value = v;
            if (v.mimeType.includes("image")) {
              // If it's an image, load the full blob
              getBlobFromCoStream({
                items: props.items,
              }).then((s) => {
                if (s) file.value = s;
              });
            }
          }
        })
        .finally(() => (isLoading.value = false));
    });

    return () => {
      if (!isLoading.value && !file.value) return <div>No blob</div>;

      if (isLoading.value) return <div>Loading...</div>;
      if (!file.value) return <div>No blob</div>;

      const { blob, mimeType } = file.value;

      const sizeInKB = (file.value.totalSize || 0) / 1024;

      return (
        <>
          <BinaryStreamGrid>
            <LabelContentPair
              label="Mime Type"
              content={(<Badge>{mimeType || "No mime type"}</Badge>) as any}
            />
            <LabelContentPair
              label="Size"
              content={(<span>{sizeInKB.toFixed(2)} KB</span>) as any}
            />
            <LabelContentPair
              label="Download"
              content={
                (
                  <BinaryDownloadButton
                    fileName={props.value.id.toString()}
                    pdfBlob={blob}
                    mimeType={mimeType}
                    label={
                      mimeType === "application/pdf"
                        ? "Download PDF"
                        : "Download file"
                    }
                  />
                ) as any
              }
            />
          </BinaryStreamGrid>
          {mimeType === "image/png" || mimeType === "image/jpeg" ? (
            <LabelContentPair
              label="Preview"
              content={
                (
                  <ImagePreviewContainer>
                    <RenderBlobImage blob={blob} />
                  </ImagePreviewContainer>
                ) as any
              }
            />
          ) : null}
        </>
      );
    };
  },
});

const RenderCoStream = defineComponent({
  name: "RenderCoStream",
  props: {
    value: {
      type: Object as PropType<RawCoStream>,
      required: true,
    },
    node: {
      type: Object as PropType<LocalNode>,
      required: true,
    },
  },
  setup(props) {
    return () => {
      const streamPerUser = Object.keys(props.value.items);
      const userCoIds = streamPerUser.map(
        (stream) => stream.split("_session")[0],
      );

      return (
        <CoStreamGrid>
          {userCoIds.map((id, idx) => (
            <CoStreamItemContainer key={id}>
              <AccountOrGroupText
                coId={id as CoID<RawCoValue>}
                node={props.node}
              />
              {/* @ts-expect-error - TODO: fix types */}
              {props.value.items[streamPerUser[idx]]?.map(
                (item: CoStreamItem<JsonValue>) => (
                  <div>
                    {new Date(item.madeAt).toLocaleString()}{" "}
                    {JSON.stringify(item.value)}
                  </div>
                ),
              )}
            </CoStreamItemContainer>
          ))}
        </CoStreamGrid>
      );
    };
  },
});

export const CoStreamView = defineComponent({
  name: "CoStreamView",
  props: {
    data: {
      type: Object as PropType<JsonObject>,
      required: true,
    },
    onNavigate: {
      type: Function as PropType<(pages: PageInfo[]) => void>,
      required: true,
    },
    node: {
      type: Object as PropType<LocalNode>,
      required: true,
    },
    value: {
      type: Object as PropType<RawCoStream>,
      required: true,
    },
  },
  setup(props) {
    return () => {
      const streamType = detectCoStreamType(props.value);

      if (streamType.type === "binary") {
        if (streamType.items === undefined) {
          return <div>No binary stream</div>;
        }

        return (
          <RenderCoBinaryStream
            value={props.value as RawBinaryCoStream}
            items={streamType.items}
          />
        );
      }

      if (streamType.type === "coStream") {
        return <RenderCoStream value={props.value} node={props.node} />;
      }

      if (streamType.type === "unknown") return <div>Unknown stream type</div>;

      return <div>Unknown stream type</div>;
    };
  },
});

const RenderBlobImage = defineComponent({
  name: "RenderBlobImage",
  props: {
    blob: {
      type: Object as PropType<Blob>,
      required: true,
    },
  },
  setup(props) {
    return () => {
      const urlCreator = window.URL || window.webkitURL;
      return <img src={urlCreator.createObjectURL(props.blob)} />;
    };
  },
});
