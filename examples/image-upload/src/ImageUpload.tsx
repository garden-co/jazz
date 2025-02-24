import { createImage } from "jazz-browser-media-images";
import { ProgressiveImg, useAccount } from "jazz-react";
import { ImageDefinition } from "jazz-tools";
import { ChangeEvent, useRef } from "react";

function Image({ image }: { image: ImageDefinition }) {
  return (
    <ProgressiveImg image={image}>
      {({ src }) => <img src={src} />}
    </ProgressiveImg>
  );
}

export default function ImageUpload() {
  const { me } = useAccount();

  const inputRef = useRef<HTMLInputElement>(null);

  const onImageChange = async (event: ChangeEvent<HTMLInputElement>) => {
    if (!me?.profile) return;

    const file = event.currentTarget.files?.[0];

    if (file) {
      me.profile.image = await createImage(file, {
        owner: me.profile._owner,
      });
    }
  };

  const deleteImage = () => {
    if (!me?.profile) return;

    me.profile.image = null;
  };

  return (
    <>
      <div>{me?.profile?.image && <Image image={me.profile.image} />}</div>

      <div>
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
              accept="image/png, image/jpeg, image/gif"
              onChange={onImageChange}
            />
          </div>
        )}
      </div>
    </>
  );
}
