import fs from "node:fs";
import { createImage } from "jazz-tools/media/server";

const image = fs.readFileSync(new URL("./image.jpg", import.meta.url));

await createImage(image, {
  // options
});
