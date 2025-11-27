import { co, ImageDefinition } from "jazz-tools";

// #region GalleryView
import { Image } from "jazz-tools/expo";
import { StyleSheet } from "react-native";

function GalleryView({ image }: { image: co.loaded<typeof ImageDefinition> }) {
  return (
    <Image
      imageId={image.$jazz.id}
      style={styles.galleryImage}
      width={400}
      resizeMode="cover"
    />
  );
}

const styles = StyleSheet.create({
  galleryImage: {
    width: "100%",
    height: 200,
    borderRadius: 8,
  },
});
// #endregion
