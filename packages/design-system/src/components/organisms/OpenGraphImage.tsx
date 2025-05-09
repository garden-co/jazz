import { ImageResponse } from "next/og";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { JazzLogo } from "../atoms/logos/JazzLogo";

export const imageSize = {
  width: 1200,
  height: 630,
};

export const imageContentType = "image/png";

async function loadManropeGoogleFont() {
  const url = `https://fonts.googleapis.com/css2?family=Manrope:wght@600`;
  const css = await (await fetch(url)).text();
  const resource = css.match(
    /src: url\((.+)\) format\('(opentype|truetype)'\)/,
  );

  if (resource) {
    const response = await fetch(resource[1]);
    if (response.status == 200) {
      return await response.arrayBuffer();
    }
  }

  throw new Error("failed to load font data");
}

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
  if (!title) {
    throw new Error(
      `No title from tocItems in opengraph-image.tsx ${framework} ${topic} ${subtopic}`,
    );
  }

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
          data: await loadManropeGoogleFont(),
          style: "normal",
          weight: 600,
        },
      ],
    },
  );
}
