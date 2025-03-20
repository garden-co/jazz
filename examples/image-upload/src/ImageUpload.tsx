import { createImage } from "jazz-browser-media-images";
import { ProgressiveImg, useAccount } from "jazz-react";
import { ChangeEvent, useEffect, useRef, useState } from "react";

export default function ImageUpload() {
  const { me } = useAccount();
  const [imagePreviewUrl, setImagePreviewUrl] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const handleBeforeUnload = (e: BeforeUnloadEvent) => {
      if (imagePreviewUrl) {
        e.preventDefault();
        return "Upload in progress. Are you sure you want to leave?";
      }
    };

    window.addEventListener("beforeunload", handleBeforeUnload);
    return () => {
      window.removeEventListener("beforeunload", handleBeforeUnload);

      if (imagePreviewUrl) {
        URL.revokeObjectURL(imagePreviewUrl);
      }
    };
  }, [imagePreviewUrl]);

  const onImageChange = async (event: ChangeEvent<HTMLInputElement>) => {
    if (!me?.profile) return;

    const file = event.currentTarget.files?.[0];

    if (file) {
      const objectUrl = URL.createObjectURL(file);
      setImagePreviewUrl(objectUrl);

      try {
        me.profile.image = await createImage(file, {
          owner: me.profile._owner,
        });
      } catch (error) {
        console.error("Error uploading image:", error);
      } finally {
        URL.revokeObjectURL(objectUrl);
        setImagePreviewUrl(null);
      }
    }
  };

  const deleteImage = () => {
    if (!me?.profile) return;
    me.profile.image = null;
  };

  if (me?.profile?.image) {
    return (
      <>
        <ProgressiveImg image={me.profile.image}>
          {({ src }) => <img alt="" src={src} className="w-full h-auto" />}
        </ProgressiveImg>

        <button type="button" onClick={deleteImage} className="mt-5">
          Delete image
        </button>
      </>
    );
  }

  if (imagePreviewUrl) {
    return (
      <div className="relative">
        <p className="z-10 absolute font-semibold text-gray-900 inset-0 flex items-center justify-center">
          Uploading image...
        </p>
        <img
          src={imagePreviewUrl}
          alt="Preview"
          className="opacity-50 w-full h-auto"
        />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <label htmlFor="image">Image</label>
      <input
        id="image"
        name="image"
        ref={inputRef}
        type="file"
        accept="image/png, image/jpeg, image/gif, image/bmp"
        onChange={onImageChange}
      />
    </div>
  );
}
