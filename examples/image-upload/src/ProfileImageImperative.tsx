import {
  highestResAvailable,
  // loadImage,
  // loadImageBySize,
} from "jazz-tools/media";
import { useAccount } from "jazz-tools/react";
import { useEffect, useState } from "react";
import { JazzAccount } from "./schema";

export default function ProfileImageImperative() {
  const [image, setImage] = useState<string | undefined>(undefined);
  const { me } = useAccount(JazzAccount, { resolve: { profile: true } });

  useEffect(() => {
    if (!me?.profile?.image) return;

    // `loadImage` returns always the original image
    // loadImage(me.profile.image).then((image) => {
    //   if(image === null) {
    //     console.error('Unable to load image');
    //     return;
    //   }
    //   console.log('loadImage', {w: image.width, h: image.height, ready: image.image.getChunks() ? 'ready' : 'not ready'});
    // });

    // `loadImageBySize` returns the best available image for the given size
    // loadImageBySize(me.profile.image.id, 1024, 1024).then((image) => {
    //   if(image === null) {
    //     console.error('Unable to load image');
    //     return;
    //   }
    //   console.log('loadImageBySize', {w: image.width, h: image.height, ready: image.image.getChunks() ? 'ready' : 'not ready'});
    // });

    // keep it synced and return the best _loaded_ image for the given size
    const unsub = me.profile.image.$jazz.subscribe({}, (image) => {
      const bestImage = highestResAvailable(image, 1024, 1024);
      console.info(bestImage ? "Blob is ready" : "Blob is not ready");
      if (bestImage) {
        const blob = bestImage.image.toBlob();
        if (blob) {
          setImage(URL.createObjectURL(blob));
        }
      }
    });

    return () => {
      unsub();
    };
  }, [me?.profile?.image]);

  const deleteImage = () => {
    if (!me?.profile) return;
    me.profile.$jazz.delete("image");
  };

  if (!me?.profile?.image) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-100 rounded-lg">
        <p className="text-gray-500">No profile image</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold">Profile Image</h2>
      <div className="border rounded-lg overflow-hidden">
        <img alt="Profile" src={image} className="w-full h-auto" />
      </div>
      <button
        type="button"
        onClick={deleteImage}
        className="bg-red-600 text-white py-2 px-3 rounded hover:bg-red-700"
      >
        Delete image
      </button>
    </div>
  );
}
