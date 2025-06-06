"use client";

import QRCodeGenerator from "qrcode";
import { useEffect, useState } from "react";
import { Button } from "./Button";

interface QRCodeProps {
  url: string;
}

export function QRCode({ url }: QRCodeProps) {
  const [qr, setQr] = useState<string>();
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    QRCodeGenerator.toDataURL(url)
      .then(setQr)
      .catch((error) => console.error(error));
  }, [url]);

  return (
    <div className="flex flex-col items-center gap-4">
      {qr ? (
        <img
          src={qr}
          alt="QR Code"
          className="w-72 h-72 rounded-xl border-2 border-blue-600"
        />
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
        {copied ? "Copied to clipboard!" : "Copy link to clipboard"}
      </Button>
    </div>
  );
}
