import { createImage } from "jazz-browser-media-images";
import { ProgressiveImg, useAccount } from "jazz-react";
import { ImageDefinition } from "jazz-tools";
import { ChangeEvent, useEffect, useRef, useState } from "react";

function Image({ image }: { image: ImageDefinition }) {
  const [isFullSize, setIsFullSize] = useState(false);

  return (
    <ProgressiveImg image={image}>
      {({ src }) => (
        <img
          src={src}
          onClick={() => setIsFullSize(!isFullSize)}
          style={{
            cursor: "pointer",
            maxWidth: isFullSize ? "none" : "80vw",
            maxHeight: isFullSize ? "none" : "80vh",
            width: "auto",
            height: "auto",
            objectFit: "contain",
          }}
          title={isFullSize ? "Click to scale down" : "Click to show full size"}
        />
      )}
    </ProgressiveImg>
  );
}

export default function ImageUpload() {
  const { me } = useAccount();
  const [isUploading, setIsUploading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const handleBeforeUnload = (e: BeforeUnloadEvent) => {
      if (isUploading) {
        e.preventDefault();
        return "Upload in progress. Are you sure you want to leave?";
      }
    };

    window.addEventListener("beforeunload", handleBeforeUnload);
    return () => window.removeEventListener("beforeunload", handleBeforeUnload);
  }, [isUploading]);

  const onImageChange = async (event: ChangeEvent<HTMLInputElement>) => {
    if (!me?.profile) return;

    const file = event.currentTarget.files?.[0];

    if (file) {
      try {
        setIsUploading(true);
        me.profile.image = await createImage(file, {
          owner: me.profile._owner,
        });
      } finally {
        setIsUploading(false);
      }
    }
  };

  const deleteImage = () => {
    if (!me?.profile) return;
    me.profile.image = null;
  };

  return (
    <>
      <div>{me?.profile?.image && <Image image={me.profile.image} />}</div>

      <div
        style={{
          display: "flex",
          justifyContent: "center",
          marginTop: "0.5rem",
        }}
      >
        {me?.profile?.image ? (
          <button type="button" onClick={deleteImage}>
            Delete image
          </button>
        ) : (
          <div>
            <label>Upload image</label>
            <input
              ref={inputRef}
              type="file"
              accept="image/png, image/jpeg, image/gif, image/bmp"
              onChange={onImageChange}
              disabled={isUploading}
            />
            {isUploading && <span>Uploading...</span>}
          </div>
        )}
      </div>
    </>
  );
}
