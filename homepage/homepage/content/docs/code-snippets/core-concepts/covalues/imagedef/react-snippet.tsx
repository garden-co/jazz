import * as react from "react";

// #region GalleryView
import { co, ImageDefinition } from "jazz-tools";
import { Image } from "jazz-tools/react";

function GalleryView({ image }: { image: co.loaded<typeof ImageDefinition> }) {
  return (
    <div className="image-container">
      <Image imageId={image.$jazz.id} alt="Profile" width={600} />
    </div>
  );
}
// #endregion

function MultiImages() {
  return (
    <>
      // #region MultiImages
      <Image imageId="123" />
      // Image with the highest resolution available
      <Image imageId="123" width="original" height="original" />
      // Image with width 1920 and height 1080
      <Image imageId="123" width={600} />
      // Better to avoid, as may be rendered with 0 height
      <Image imageId="123" width={600} height="original" />
      // Keeps the aspect ratio (height: 338)
      <Image imageId="123" width="original" height={600} />
      // As above, aspect ratio is maintained, width is 1067
      <Image imageId="123" width={600} height={600} />
      // Renders as a 600x600 square // #endregion
    </>
  );
}

function LazyLoad() {
  return (
    <>
      // #region LazyLoad
      <Image imageId="123" width="original" height="original" loading="lazy" />
      // #endregion
    </>
  );
}
