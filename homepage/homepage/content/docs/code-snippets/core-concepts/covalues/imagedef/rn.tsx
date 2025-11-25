import { co, ImageDefinition, z } from "jazz-tools";

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
import { createImage } from "jazz-tools/media";
import { launchImageLibrary } from "react-native-image-picker";

async function handleImagePicker() {
  // Use your favorite image picker library to get the image URI
  const result = await launchImageLibrary({
    mediaType: "photo",
    quality: 1,
  });

  if (
    !result.didCancel &&
    result.assets &&
    result.assets.length > 0 &&
    me.profile.$isLoaded
  ) {
    // Creates ImageDefinition with a blurry placeholder, limited to 1024px on the longest side, and multiple resolutions automatically.
    // See the options below for more details.
    const image = await createImage(result.assets[0].uri ?? "", {
      owner: me.$jazz.owner,
      maxSize: 1024,
      placeholder: "blur",
      progressive: true,
    });

    // Store the image
    me.profile.$jazz.set("image", image);
  }
}
// #endregion
