import { Button } from "@/basicComponents";
import QRCodeGenerator from "qrcode";
import { useEffect, useState } from "react";

interface QRCodeProps {
  url: string;
}

export function QRCode({ url }: QRCodeProps) {
  const [qr, setQr] = useState<string | undefined>();
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    setQr(undefined);
    QRCodeGenerator.toDataURL(url)
      .then(setQr)
      .catch((error) => console.error(error));
  }, [url]);

  return (
    <div className="flex flex-col items-center gap-2">
      {qr ? <img src={qr} alt="QR Code" className="w-60 h-60" /> : null}

      <Button
        variant="link"
        onClick={() => {
          navigator.clipboard.writeText(url);
          setCopied(true);
        }}
      >
        {copied ? "Copied!" : "Copy link"}
      </Button>
    </div>
  );
}
