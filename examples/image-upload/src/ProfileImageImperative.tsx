import { useAccount } from "jazz-tools/react";
import { JazzAccount } from "./schema";
import { useEffect, useState } from "react";

export default function ProfileImageImperative() {
  const [image, setImage] = useState<string | undefined>(undefined);
  const { me } = useAccount(JazzAccount, { resolve: { profile: true } });

  useEffect(() => {
    if (!me?.profile?.image) return;

    // fetch it once
    // loadImageBlob(me.profile.image.id).then(({ image }) => {
    //   setImage(URL.createObjectURL(image));
    // }).catch(console.error);

    // keep it synced
    const unsub = me.profile.image.subscribe({}, (update) => {
      const blob = update['original']?.toBlob();
      if(blob) {
        setImage(URL.createObjectURL(blob));
      }
    });

    return () => {
      unsub();
    };
  }, [me?.profile?.image])

  const deleteImage = () => {
    if (!me?.profile) return;
    me.profile.image = undefined;
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
