import React, { useState } from "react";
import { Image, useSuspenseAccount } from "jazz-tools/react";
import { createImage } from "jazz-tools/media";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Separator } from "./ui/separator";
import { Group } from "jazz-tools";
import { MusicaAccount } from "@/1_schema";

interface ProfileFormProps {
  onSubmit?: (data: { username: string; avatar?: any }) => void;
  submitButtonText?: string;
  showHeader?: boolean;
  headerTitle?: string;
  headerDescription?: string;
  onCancel?: () => void;
  showCancelButton?: boolean;
  cancelButtonText?: string;
  className?: string;
}

export function ProfileForm({
  onSubmit,
  submitButtonText = "Save Changes",
  showHeader = false,
  headerTitle = "Profile Settings",
  headerDescription = "Update your profile information",
  onCancel,
  showCancelButton = false,
  cancelButtonText = "Cancel",
  className = "",
}: ProfileFormProps) {
  const originalProfile = useSuspenseAccount(MusicaAccount, {
    select: (me) => me.profile,
  });

  const profile = useSuspenseAccount(MusicaAccount, {
    select: (me) => me.profile,
    // Edit the profile on a private branch
    unstable_branch: {
      name: "profile-form",
      owner: useState(() => Group.create())[0], // Create a new group for the branch
    },
  });

  const [isUploading, setIsUploading] = useState(false);

  if (!profile) return null;

  const handleAvatarUpload = async (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const file = event.target.files?.[0];
    if (!file) return;

    setIsUploading(true);
    try {
      // Create image using the Image API from jazz-tools/media
      const image = await createImage(file, {
        owner: Group.create().makePublic(),
        maxSize: 256, // Good size for avatars
        placeholder: "blur",
        progressive: true,
      });

      // Update the profile with the new avatar
      profile.$jazz.set("avatar", image);
    } catch (error) {
      console.error("Failed to upload avatar:", error);
    } finally {
      setIsUploading(false);
    }
  };

  const currentAvatar = profile.avatar;
  const isAvatarChanged =
    currentAvatar?.$jazz.id !== originalProfile.avatar?.$jazz.id;
  const isNameChanged = profile.name !== originalProfile.name;
  const isChanged = isAvatarChanged || isNameChanged;
  const isSubmitEnabled = profile.name.trim() && !isUploading && isChanged;

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();

    if (!isSubmitEnabled) return;

    // Trim the name before merging
    const name = profile.name.trim();
    if (name !== profile.name) {
      profile.$jazz.set("name", name);
    }

    // Merge the branch changes to confirm
    profile.$jazz.unstable_merge();

    // Call custom onSubmit if provided
    if (onSubmit) {
      onSubmit({ username: profile.name, avatar: profile.avatar });
    }
  };

  return (
    <div className={className}>
      {showHeader && (
        <div className="text-center mb-8">
          <h1 className="text-2xl font-bold text-gray-900 mb-2">
            {headerTitle}
          </h1>
          <p className="text-gray-600">{headerDescription}</p>
        </div>
      )}

      <form className="space-y-6" onSubmit={handleSubmit}>
        {/* Avatar Section */}
        <div className="space-y-3">
          <Label
            htmlFor="avatar"
            className="text-sm font-medium text-gray-700 sr-only"
          >
            Profile Picture
          </Label>

          <label
            htmlFor="avatar"
            className="flex flex-col items-center space-y-3"
          >
            {/* Current Avatar Display */}
            <div className="relative">
              <div className="w-24 h-24 rounded-full overflow-hidden border-4 border-white shadow-lg">
                {currentAvatar ? (
                  <Image
                    imageId={currentAvatar.$jazz.id}
                    width={96}
                    height={96}
                    alt="Profile"
                    className="w-full h-full object-cover"
                  />
                ) : (
                  <div className="w-full h-full bg-gray-200 flex items-center justify-center">
                    <span className="text-gray-400 text-2xl">👤</span>
                  </div>
                )}
              </div>

              {/* Upload Overlay */}
              <button
                type="button"
                disabled={isUploading}
                className="absolute -bottom-1 -right-1 w-8 h-8 bg-blue-500 rounded-full flex items-center justify-center text-white hover:bg-blue-700 disabled:opacity-50 transition-colors cursor-pointer"
                title="Change avatar"
              >
                {isUploading ? (
                  <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                ) : (
                  <span className="text-sm">📷</span>
                )}
              </button>
            </div>

            <input
              type="file"
              id="avatar"
              accept="image/*"
              onChange={handleAvatarUpload}
              className="hidden"
              disabled={isUploading}
            />

            <p className="text-xs text-gray-500 text-center">
              Click the camera icon to upload a profile picture
            </p>
          </label>
        </div>

        <Separator />

        {/* Username Section */}
        <div className="space-y-3">
          <Label
            htmlFor="username"
            className="text-sm font-medium text-gray-700"
          >
            Username
          </Label>
          <Input
            id="username"
            type="text"
            placeholder="Enter your username"
            value={profile.name}
            onChange={(e) => profile.$jazz.set("name", e.target.value)}
            className="w-full"
            maxLength={30}
          />
          <p className="text-xs text-gray-500">
            This will be displayed to other users
          </p>
        </div>

        {/* Action Buttons */}
        <div className="flex space-x-3">
          {showCancelButton && (
            <Button
              type="button"
              variant="outline"
              onClick={onCancel}
              className="flex-1"
              size="lg"
            >
              {cancelButtonText}
            </Button>
          )}
          <Button
            type="submit"
            disabled={!isSubmitEnabled}
            className={`${showCancelButton ? "flex-1" : "w-full"} bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed`}
            size="lg"
          >
            {submitButtonText}
          </Button>
        </div>
      </form>
    </div>
  );
}
