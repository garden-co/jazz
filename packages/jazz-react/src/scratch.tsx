import { FileStream, ImageDefinition } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";

// Simple document environment
global.document = {
  createElement: () =>
    ({ src: "", onload: null }) as unknown as HTMLImageElement,
} as unknown as Document;
global.window = { innerWidth: 1000 } as unknown as Window & typeof globalThis;

const me = await createJazzTestAccount();

const mediumSizeBlob = new Blob([], { type: "image/jpeg" });
const image = ImageDefinition.create(
  {
    originalSize: [1920, 1080],
  },
  { owner: me },
);
image["100x100"] = await FileStream.createFromBlob(mediumSizeBlob, {
  owner: me,
});
image["1920x1080"] = await FileStream.createFromBlob(mediumSizeBlob, {
  owner: me,
});
const imageElement = document.createElement("img");
// ---cut---
// Start with placeholder for immediate display
if (image.placeholderDataURL) {
  imageElement.src = image.placeholderDataURL;
}

// Then load the best resolution for the current display
const screenWidth = window.innerWidth;
const bestRes = ImageDefinition.highestResAvailable(image, {
  targetWidth: screenWidth,
});

if (bestRes) {
  const blob = bestRes.stream.toBlob();
  if (blob) {
    const url = URL.createObjectURL(blob);
    imageElement.src = url;

    // Remember to revoke the URL when no longer needed
    imageElement.onload = () => {
      URL.revokeObjectURL(url);
    };
  }
}
