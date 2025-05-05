import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { ImageResponse } from "next/og";
import { JazzLogo } from "../atoms/logos/JazzLogo";

export const imageSize = {
  width: 1200,
  height: 630,
};

export const imageContentType = "image/png";

export async function OpenGraphImage({
  title,
  framework,
  contents,
  topic,
  subtopic,
}: {
  title: string;
  framework?: string;
  contents?: string[];
  topic?: string;
  subtopic?: string;
}) {
  const manropeSemiBold = await readFile(
    join(process.cwd(), "public/fonts/Manrope-SemiBold.ttf"),
  );

  return new ImageResponse(
    <div
      style={{
        background: "white",
        width: "100%",
        height: "100%",
        display: "flex",
        flexDirection: "column",
        alignItems: "flex-start",
        justifyContent: "center",
        padding: "77px",
        letterSpacing: "-0.05em",
      }}
    >
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          gap: "1rem",
          fontSize: "4rem",
        }}
      >
        {title}
      </div>
      <div
        style={{
          position: "absolute",
          right: 15,
          top: 10,
          display: "flex",
          alignItems: "center",
          justifyContent: "flex-start",
          fontSize: "3rem",
          color: "#888888",
          letterSpacing: "-0.03em",
        }}
      >
        <div style={{ display: "flex", color: "#BBB", paddingRight: "0.5rem" }}>
          {framework}
        </div>
        {topic && (
          <span style={{ color: "#CCC", paddingRight: "0.5rem" }}>
            / {topic}
          </span>
        )}
        {subtopic && <span style={{ color: "#DDD" }}>/ {subtopic}</span>}
      </div>
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          marginTop: "1rem",
          gap: "0.2rem",
          fontSize: "2rem",
          color: "#888888",
          letterSpacing: "-0.03em",
        }}
      >
        {contents?.map((content) => (
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
