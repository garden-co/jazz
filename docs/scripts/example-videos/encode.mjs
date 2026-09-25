// Composes per-device Playwright recordings into one streaming-friendly H.264
// MP4 (plays in every browser, including Safari) plus a JPEG poster, under a
// size budget. The stage (Safari-style windows, labels, captions) is drawn by
// the browser in the docs site's body font and layered by ffmpeg (libx264).
import { execFile } from "node:child_process";
import { readFile, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";

const run = promisify(execFile);
export const MAX_BYTES = 5_000_000;

const fontFile = (weight) =>
  new URL(`../../public/fonts/body-font-latin-${weight}-normal.woff2`, import.meta.url);
/** The docs site's body font (Akkurat), for the stage around the recordings. */
const stageFont = { family: "Akkurat", files: { 400: fontFile(400), 700: fontFile(700) } };

const background = "#0b0d10";
const layout = { pad: 32, gap: 28, captionBar: 84, label: 26, titleBar: 44, radius: 12 };

async function fontFaces() {
  const faces = [];
  for (const [weight, file] of Object.entries(stageFont.files)) {
    const data = (await readFile(file)).toString("base64");
    faces.push(
      `@font-face{font-family:${stageFont.family};font-weight:${weight};src:url(data:font/woff2;base64,${data}) format("woff2")}`,
    );
  }
  return faces.join("");
}

const escape = (value) =>
  value.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);

function geometry(count, size) {
  const { pad, gap, captionBar, label, titleBar } = layout;
  const even = (n) => n + (n % 2);
  const windowY = captionBar + label;
  return {
    width: even(pad * 2 + count * size.width + (count - 1) * gap),
    height: even(windowY + titleBar + size.height + pad),
    windowY,
    contentY: windowY + titleBar,
    x: (i) => pad + i * (size.width + gap),
  };
}

/** Renders the stage layers as PNGs: under the videos, over them, and one per caption. */
async function renderStage({ browser, devices, captions, size, address, dir }) {
  const g = geometry(devices.length, size);
  const { radius, titleBar, label } = layout;
  const page = await browser.newPage({ viewport: { width: g.width, height: g.height } });
  const base = `<style>${await fontFaces()}
    *{box-sizing:border-box;margin:0}
    html,body{width:${g.width}px;height:${g.height}px;overflow:hidden;font-family:${stageFont.family},sans-serif;-webkit-font-smoothing:antialiased}
    .abs{position:absolute}
  </style>`;
  const shot = async (html, name, transparent) => {
    await page.setContent(`<!doctype html><html><head>${base}</head><body>${html}</body></html>`);
    await page.evaluate(() => document.fonts.ready);
    const path = join(dir, `${name}.png`);
    await page.screenshot({ path, omitBackground: transparent });
    return path;
  };
  const windows = devices
    .map((d, i) => {
      const x = g.x(i);
      return `
        <div class="abs" style="left:${x}px;top:${g.windowY - label}px;width:${size.width}px;color:#8b919c;font-size:15px;letter-spacing:.01em">${escape(d.label)}</div>
        <div class="abs" style="left:${x}px;top:${g.windowY}px;width:${size.width}px;height:${titleBar + size.height}px;border-radius:${radius}px;background:#000;box-shadow:0 18px 50px rgba(0,0,0,.55)"></div>
        <div class="abs" style="left:${x}px;top:${g.windowY}px;width:${size.width}px;height:${titleBar}px;border-radius:${radius}px ${radius}px 0 0;background:#26282d;border-bottom:1px solid #1a1b1f">
          ${["#55585e", "#55585e", "#55585e"].map((c, j) => `<span class="abs" style="left:${16 + j * 20}px;top:16px;width:12px;height:12px;border-radius:50%;background:${c}"></span>`).join("")}
          <div class="abs" style="left:50%;top:9px;transform:translateX(-50%);width:56%;height:26px;border-radius:7px;background:#36393f;color:#c9ccd1;font-size:13px;line-height:26px;text-align:center">${escape(address)}</div>
        </div>`;
    })
    .join("");
  const under = await shot(
    `<div class="abs" style="inset:0;background:${background}"></div>${windows}`,
    "under",
    false,
  );
  // Over the videos: round the content's bottom corners and add a hairline edge.
  const over = await shot(
    devices
      .map((_, i) => {
        const x = g.x(i);
        const bottom = g.contentY + size.height;
        return `
          <div class="abs" style="left:${x}px;top:${bottom - radius}px;width:${size.width}px;height:${radius}px;overflow:hidden">
            <div class="abs" style="left:0;top:${-radius}px;width:${size.width}px;height:${radius * 2}px;border-radius:0 0 ${radius}px ${radius}px;box-shadow:0 0 0 ${radius * 2}px ${background}"></div>
          </div>
          <div class="abs" style="left:${x}px;top:${g.windowY}px;width:${size.width}px;height:${titleBar + size.height}px;border-radius:${radius}px;border:1px solid rgba(255,255,255,.12)"></div>`;
      })
      .join(""),
    "over",
    true,
  );
  const captionImages = [];
  for (const [i, c] of captions.entries()) {
    captionImages.push(
      await shot(
        `<div class="abs" style="left:0;top:0;width:100%;height:${layout.captionBar}px;display:flex;align-items:center;justify-content:center;color:#eceef2;font-size:28px;letter-spacing:-.005em">${escape(c.text)}</div>`,
        `caption-${i}`,
        true,
      ),
    );
  }
  await page.close();
  return { g, under, over, captionImages };
}

/**
 * devices: [{ path, label, startedAt }] recorded side by side; captions:
 * [{ text, from, to }] in ms since `origin` (the moment every device was ready).
 */
export async function composeWindows({
  browser,
  devices,
  captions,
  origin,
  end,
  outDir,
  id,
  size,
  address,
  workDir,
}) {
  const mp4 = `${outDir}${id}.mp4`;
  const poster = `${outDir}${id}.jpg`;
  const duration = ((end - origin) / 1000).toFixed(3);
  const { g, under, over, captionImages } = await renderStage({
    browser,
    devices,
    captions,
    size,
    address,
    dir: workDir,
  });
  const still = (path) => ["-loop", "1", "-framerate", "30", "-t", duration, "-i", path];
  const inputs = [
    ...devices.flatMap((d) => [
      "-ss",
      String(Math.max(0, (origin - d.startedAt) / 1000)),
      "-i",
      d.path,
    ]),
    ...still(under),
    ...still(over),
    ...captionImages.flatMap(still),
  ];
  const n = devices.length;
  const filters = devices.map(
    (_, i) => `[${i}:v]setpts=PTS-STARTPTS,fps=30,scale=${size.width}:${size.height}[v${i}]`,
  );
  let last = `${n}:v`;
  devices.forEach((_, i) => {
    filters.push(`[${last}][v${i}]overlay=x=${g.x(i)}:y=${g.contentY}:shortest=1[d${i}]`);
    last = `d${i}`;
  });
  filters.push(`[${last}][${n + 1}:v]overlay=0:0[framed]`);
  last = "framed";
  captions.forEach((c, i) => {
    const on = `between(t,${(c.from / 1000).toFixed(2)},${(c.to / 1000).toFixed(2)})`;
    filters.push(`[${last}][${n + 2 + i}:v]overlay=0:0:enable='${on}'[c${i}]`);
    last = `c${i}`;
  });
  const filterFile = join(workDir, "filter.txt");
  await writeFile(filterFile, filters.join(";\n"));
  // Screen recordings compress well; raise CRF until the clip fits the budget.
  for (const crf of [26, 30, 34, 38]) {
    await run(
      "ffmpeg",
      [
        "-y",
        "-loglevel",
        "error",
        ...inputs,
        "-filter_complex_script",
        filterFile,
        "-map",
        `[${last}]`,
        "-t",
        duration,
        "-c:v",
        "libx264",
        "-preset",
        "slow",
        "-crf",
        String(crf),
        "-pix_fmt",
        "yuv420p",
        "-an",
        "-movflags",
        "+faststart",
        mp4,
      ],
      { maxBuffer: 16 * 1024 * 1024 },
    );
    const { size: bytes } = await stat(mp4);
    if (bytes <= MAX_BYTES) {
      // The poster is the final frame: both devices showing the synced end state.
      await run("ffmpeg", [
        "-y",
        "-loglevel",
        "error",
        "-sseof",
        "-0.5",
        "-i",
        mp4,
        "-frames:v",
        "1",
        "-q:v",
        "3",
        poster,
      ]);
      return { mp4, poster, bytes, crf };
    }
  }
  throw new Error(`${id}: could not encode under ${MAX_BYTES} bytes`);
}
