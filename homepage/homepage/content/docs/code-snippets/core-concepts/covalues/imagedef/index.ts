// @ts-expect-error duplicate import
import { Account, co, Group, ImageDefinition, Loaded, z } from "jazz-tools";

const JazzAccount = co.account({
  profile: co.profile({
    name: z.string(),
    image: ImageDefinition,
  }),
  root: co.map({}),
});

const me = JazzAccount.getMe();
await me.$jazz.ensureLoaded({
  resolve: {
    profile: {
      image: true,
    },
  },
});

// #region Basic
// @ts-expect-error TODO: I know the type conflicts below.
import { createImage } from "jazz-tools/media";

// Create an image from a file input
async function handleFileUpload(event: Event) {
  const input = event.target as HTMLInputElement | null;
  const file = input?.files?.[0];
  if (file && me.profile.$isLoaded) {
    // Creates ImageDefinition with a blurry placeholder, limited to 1024px on the longest side, and multiple resolutions automatically
    const image = await createImage(file, {
      owner: me.$jazz.owner,
      maxSize: 1024,
      placeholder: "blur",
      progressive: true,
    });

    // Store the image in your application data
    me.profile.$jazz.set("image", image);
  }
}
// #endregion

const myBlob = new Blob();
// #region CreateResized
// @ts-expect-error duplicate import
import { co } from "jazz-tools";
// @ts-expect-error duplicate import
import { createImage } from "jazz-tools/media";

// Jazz Schema
const ProductImage = co.map({
  image: co.image(),
  thumbnail: co.image(),
});

const mainImage = await createImage(myBlob);
const thumbnail = await createImage(myBlob, {
  maxSize: 100,
});

// or, in case of migration, you can use the original stored image.
const newThumb = await createImage(mainImage!.original!.toBlob()!, {
  maxSize: 100,
});

const imageSet = ProductImage.create({
  image: mainImage,
  thumbnail,
});
// #endregion

// #region Imperative
const image = await ImageDefinition.load("123", {
  resolve: {
    original: true,
  },
});

if (image.$isLoaded) {
  console.log({
    originalSize: image.originalSize,
    placeholderDataUrl: image.placeholderDataURL,
    original: image.original, // this FileStream may be not loaded yet
  });
}
// #endregion

const imageDefinitionOrId = "";

// #region LoadImageHelper
import { loadImage } from "jazz-tools/media";

const loadedImage = await loadImage(imageDefinitionOrId);
if (loadedImage === null) {
  throw new Error("Image not found");
}

// @ts-expect-error redeclared
const img = document.createElement("img");
img.width = loadedImage.width;
img.height = loadedImage.height;
img.src = URL.createObjectURL(loadedImage.image.toBlob()!);
img.onload = () => URL.revokeObjectURL(img.src);
// #endregion

// #region LoadImageBySize
import { loadImageBySize } from "jazz-tools/media";

const imageLoadedBySize = await loadImageBySize(imageDefinitionOrId, 600, 600); // 600x600

if (imageLoadedBySize) {
  console.log({
    width: imageLoadedBySize.width,
    height: imageLoadedBySize.height,
    image: imageLoadedBySize.image,
  });
}
// #endregion

const imageId = "";
// #region HighestResAvailable
import { highestResAvailable } from "jazz-tools/media";

const progressiveImage = await ImageDefinition.load(imageId);

if (!progressiveImage.$isLoaded) {
  throw new Error("Image not loaded");
}

// @ts-expect-error redeclared
const img = document.createElement("img");
img.width = 600;
img.height = 600;

// start with the placeholder
if (progressiveImage.placeholderDataURL) {
  img.src = progressiveImage.placeholderDataURL;
}

// then listen to the image changes
progressiveImage.$jazz.subscribe({}, (image) => {
  // @ts-expect-error Issue with typing for catch-all. No runtime impact.
  const bestImage = highestResAvailable(image, 600, 600);

  if (bestImage) {
    // bestImage is again a FileStream
    const blob = bestImage.image.toBlob();
    if (blob) {
      const url = URL.createObjectURL(blob);
      img.src = url;
      img.onload = () => URL.revokeObjectURL(url);
    }
  }
});
// #endregion

// #region CreateImageFactory
import { createImageFactory } from "jazz-tools/media";

const customCreateImage = createImageFactory({
  // @ts-expect-error illustration
  createFileStreamFromSource: async (source, owner) => {
    // ...
  },
  // @ts-expect-error illustration
  getImageSize: async (image) => {
    // ...
  },
  // @ts-expect-error illustration
  getPlaceholderBase64: async (image) => {
    // ...
  },
  resize: async (image, width, height) => {
    // ...
  },
});
// #endregion
