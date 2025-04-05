"use client";

import QRCode from "qrcode";
import { useEffect, useState } from "react";
import { Button } from "./Button";

interface QRCodeContainerProps {
  url: string;
}

export function QRCodeContainer({ url }: QRCodeContainerProps) {
  const [qr, setQr] = useState<string>();
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    QRCode.toDataURL(url)
      .then(setQr)
      .catch((error) => console.error(error));
  }, [url]);

  return (
    <div className="border-2 border-blue-600 p-6 rounded-lg flex flex-col items-center gap-4">
      {qr ? (
        <img src={qr} alt="QR Code" className="w-72 h-72 rounded" />
      ) : (
        <div className="w-96 h-96 bg-white rounded-lg flex items-center justify-center">
          Loading...
        </div>
      )}

      <Button
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
