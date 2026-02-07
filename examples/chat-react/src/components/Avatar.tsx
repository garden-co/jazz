import type { ID } from "jazz-tools";
import multiavatar from "@multiavatar/multiavatar/esm";
import { createImage } from "jazz-tools/media";
import {
  Image,
  useSuspenseAccount,
  useSuspenseCoState,
} from "jazz-tools/react";
import {
  AvatarFallback,
  AvatarImage,
  Avatar as ShadCNAvatar,
} from "@/components/ui/avatar";
import { Button } from "@/components/ui/button";
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field";
import { ChatAccountWithProfile, ChatProfile } from "@/schema";

interface AvatarProps {
  profileId?: ID<typeof ChatProfile>;
  editable?: boolean;
  className?: string;
}

export const Avatar = ({ profileId, editable, className }: AvatarProps) => {
  const me = useSuspenseAccount(ChatAccountWithProfile);
  const actualProfileId = profileId || me.profile.$jazz.id;

  const profile = useSuspenseCoState(ChatProfile, actualProfileId, {
    resolve: {
      avatar: true,
    },
  });

  const avatarSvg = multiavatar(actualProfileId);
  const dataUrl = `data:image/svg+xml;base64,${btoa(avatarSvg)}`;

  const avatarDisplay = (
    <ShadCNAvatar size="lg" className={className}>
      {profile.avatar ? (
        <Image
          imageId={profile.avatar.$jazz.id}
          width={128}
          height="original"
          className="object-cover"
        />
      ) : (
        <AvatarImage src={dataUrl} alt="Avatar" />
      )}
      <AvatarFallback>
        {profile.name
          ? profile.name
              .split(" ")
              .map((n: string) => n[0])
              .join("")
          : "?"}
      </AvatarFallback>
    </ShadCNAvatar>
  );

  if (!editable) {
    return avatarDisplay;
  }

  return (
    <Field>
      <FieldLabel htmlFor="avatar">Avatar</FieldLabel>
      <FieldDescription>Upload a profile picture.</FieldDescription>
      <div className="flex items-center gap-2">
        <label className="cursor-pointer transition-opacity hover:opacity-80">
          {avatarDisplay}
          <input
            type="file"
            className="hidden"
            id="avatar"
            accept="image/*"
            onChange={async (evt) => {
              if (!evt.target.files) return;
              const file = evt.target.files[0];
              const img = await createImage(file, {
                progressive: true,
                maxSize: 256,
                placeholder: "blur",
              });
              profile.$jazz.set("avatar", img);
            }}
          />
        </label>

        {profile.avatar && (
          <Button
            variant="outline"
            onClick={() => profile.$jazz.delete("avatar")}
          >
            Remove
          </Button>
        )}
      </div>
    </Field>
  );
};
