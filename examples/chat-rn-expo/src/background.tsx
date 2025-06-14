import * as ImagePicker from "expo-image-picker";
import { ProgressiveImg } from "jazz-expo";
import { createImage } from "jazz-react-native-media-images";
import { Group, ImageDefinition } from "jazz-tools";
import React, { useState } from "react";
import { Dimensions, Pressable, StyleSheet, Text, View } from "react-native";
import Svg, { Defs, Mask, Path, Image as SvgImage } from "react-native-svg";
import { ChatAccount } from "./schema";

function MaskedBackgroundPhoto({ imageUri }: { imageUri: string }) {
  const windowWidth = Dimensions.get("window").width;
  const height = Dimensions.get("window").height * 0.4;

  return (
    <View style={[styles.backgroundContainer, { width: windowWidth, height }]}>
      <Svg height={height} width={windowWidth}>
        <Defs>
          <Mask id="mask" x="0" y="0" height="100%" width="100%">
            <Path
              d={`M0 0 L${windowWidth} 0 L${windowWidth} ${height - 40} Q${windowWidth / 2} ${height + 50} 0 ${height - 40} Z`}
              fill="white"
            />
          </Mask>
        </Defs>
        <SvgImage
          width="100%"
          height="100%"
          href={{ uri: imageUri }}
          preserveAspectRatio="xMidYMid slice"
          mask="url(#mask)"
        />
      </Svg>
    </View>
  );
}

export function BackgroundPhoto({
  image,
  owner,
}: {
  image: ImageDefinition | null;
  owner: ChatAccount | Group | undefined | null;
}) {
  const [img, setImg] = useState<ImageDefinition | null>(image);

  const handleImageUpload = async () => {
    try {
      const result = await ImagePicker.launchImageLibraryAsync({
        mediaTypes: ["images"],
        base64: true,
        quality: 0.8,
      });
      if (!result.canceled && owner && result.assets[0].base64) {
        const base64Uri = `data:image/jpeg;base64,${result.assets[0].base64}`;
        const img = await createImage(base64Uri, {
          owner,
          maxSize: 2048,
        });
        setImg(img);
      }
    } catch (error) {
      console.error("Failed to upload image:", error);
    }
  };

  return (
    <Pressable onPress={handleImageUpload}>
      {img ? (
        <ProgressiveImg
          image={img}
          targetWidth={Dimensions.get("window").width}
        >
          {({ src }) => (src ? <MaskedBackgroundPhoto imageUri={src} /> : null)}
        </ProgressiveImg>
      ) : (
        <View style={styles.noPhotoContainer}>
          <Text style={styles.noPhotoText}>No background photo</Text>
        </View>
      )}
    </Pressable>
  );
}

const styles = StyleSheet.create({
  backgroundContainer: {
    position: "relative",
  },
  noPhotoContainer: {
    width: "100%",
    height: Dimensions.get("window").height * 0.2,
    backgroundColor: "lightgray",
    justifyContent: "center",
  },
  noPhotoText: {
    textAlign: "center",
  },
});
