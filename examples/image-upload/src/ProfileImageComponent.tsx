import { Image, useAccount } from "jazz-tools/react";
import { JazzAccount } from "./schema";

export default function ProfileImage() {
  const { me } = useAccount(JazzAccount, { resolve: { profile: true } });

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
        <Image imageId={me.profile.image.$jazz.id} alt="Profile" width={600} />
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
