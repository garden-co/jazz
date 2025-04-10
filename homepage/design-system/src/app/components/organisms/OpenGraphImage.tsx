import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { ImageResponse } from "next/og";
import { JazzLogo } from "../atoms/logos/JazzLogo";

export const imageSize = {
  width: 1200,
  height: 630,
};

export const imageContentType = "image/png";

export async function OpenGraphImage({ title }: { title: string }) {
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
        style={{
          display: "flex",
          position: "absolute",
          bottom: 35,
          right: 45,
        }}
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

export async function DocsOpenGraphImage({
  title,
  framework,
  contents,
}: {
  title: string;
  framework: string;
  contents: string[];
}) {
  const manropeSemiBold = await readFile(
    join(process.cwd(), "public/fonts/Manrope-SemiBold.ttf"),
  );

  return new ImageResponse(
    <div
      style={{
        fontSize: "7rem",
        background: "white",
        width: "100%",
        height: "100%",
        display: "flex",
        flexDirection: "column",
        alignItems: "flex-start",
        justifyContent: "flex-start",
        padding: "77px",
        letterSpacing: "-0.05em",
      }}
    >
      <div
        style={{
          display: "flex",
          flexDirection: "row",
          alignItems: "center",
          gap: "1rem",
        }}
      >
        {title}
        <span style={{ color: "#DDDDDD" }}>({framework})</span>
      </div>
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          marginTop: "1rem",
          gap: "0.2rem",
          fontSize: "3rem",
          color: "#888888",
          letterSpacing: "-0.03em",
        }}
      >
        {contents.map((content) => (
          <div key={content}>{content}</div>
        ))}
      </div>
      <div
        style={{
          display: "flex",
          position: "absolute",
          bottom: 35,
          right: 45,
        }}
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
