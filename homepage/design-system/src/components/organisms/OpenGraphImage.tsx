import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { ImageResponse } from "next/og";
import { JazzLogo } from "../atoms/logos/JazzLogo";

export const imageSize = {
  width: 1200,
  height: 630,
};

export const imageContentType = "image/png";

export default async function OpenGraphImage({ title }: { title: string }) {
  const manropeSemiBold = await readFile(
    join(process.cwd(), "public/fonts/Manrope-SemiBold.ttf"),
  );

  return new ImageResponse(
    <div
      style={{
        fontSize: "7em",
        background: "white",
        width: "100%",
        height: "100%",
        display: "flex",
        alignItems: "center",
        justifyContent: "flex-start",
        padding: "77px",
        letterSpacing: "-0.05em",
      }}
    >
      {title}
      <div
        style={{ display: "flex", position: "absolute", bottom: 35, right: 45 }}
      >
        <JazzLogo width={193} height={73} />
      </div>
    </div>,
    {
      ...imageSize,
      fonts: [
        {
          name: "Manrope",
          data: manropeSemiBold,
        },
      ],
    },
  );
}
