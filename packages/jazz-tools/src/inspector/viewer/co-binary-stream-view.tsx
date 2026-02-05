import { RawBinaryCoStream } from "cojson";
import { styled } from "goober";
import { useEffect, useState } from "react";
import { Badge } from "../ui/badge.js";
import { Button } from "../ui/button.js";

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

const LabelContentPair = ({
  label,
  content,
}: {
  label: string;
  content: React.ReactNode;
}) => {
  return (
    <LabelContentPairContainer>
      <span>{label}</span>
      <span>{content}</span>
    </LabelContentPairContainer>
  );
};

function RenderBlobImage({ blob }: { blob: Blob }) {
  const urlCreator = window.URL || window.webkitURL;
  return <img src={urlCreator.createObjectURL(blob)} />;
}

export function CoBinaryStreamView({ value }: { value: RawBinaryCoStream }) {
  const [file, setFile] = useState<
    | {
        blob: Blob;
        mimeType: string;
        totalSize: number | undefined;
      }
    | undefined
    | null
  >(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    async function loadBinaryStream() {
      const data = value.getBinaryChunks(true);
      if (!data) {
        setIsLoading(false);
        return;
      }

      const blob = new Blob(data.chunks, { type: data.mimeType });
      let mimeType = data.mimeType;
      if (mimeType === "") {
        mimeType = await detectPDFMimeType(blob);
      }

      setFile({
        blob,
        mimeType,
        totalSize: data.totalSizeBytes,
      });
      setIsLoading(false);
    }

    loadBinaryStream();
  }, [value]);

  if (isLoading) return <div>Loading...</div>;
  if (!file) return <div>No blob</div>;

  const { blob, mimeType } = file;
  const sizeInKB = (file.totalSize || 0) / 1024;

  return (
    <>
      <BinaryStreamGrid>
        <LabelContentPair
          label="Mime Type"
          content={<Badge>{mimeType || "No mime type"}</Badge>}
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
                  : "Download file"
              }
            />
          }
        />
      </BinaryStreamGrid>
      {mimeType === "image/png" || mimeType === "image/jpeg" ? (
        <LabelContentPair
          label="Preview"
          content={
            <ImagePreviewContainer>
              <RenderBlobImage blob={blob} />
            </ImagePreviewContainer>
          }
        />
      ) : null}
    </>
  );
}
