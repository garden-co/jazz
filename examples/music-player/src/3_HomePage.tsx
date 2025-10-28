import { useCoState } from "jazz-tools/react";
import { useParams } from "react-router";
import { Playlist } from "./1_schema";
import { uploadMusicTracks } from "./4_actions";
import { MediaPlayer } from "./5_useMediaPlayer";
import { FileUploadButton } from "./components/FileUploadButton";
import { MusicTrackRow } from "./components/MusicTrackRow";
import { PlayerControls } from "./components/PlayerControls";
import { EditPlaylistModal } from "./components/EditPlaylistModal";
import { PlaylistMembers } from "./components/PlaylistMembers";
import { MemberAccessModal } from "./components/MemberAccessModal";
import { SidePanel } from "./components/SidePanel";
import { Button } from "./components/ui/button";
import { SidebarInset, SidebarTrigger } from "./components/ui/sidebar";
import { usePlayState } from "./lib/audio/usePlayState";
import { useState } from "react";
import { useAccountSelector } from "@/components/AccountProvider.tsx";

export function HomePage({ mediaPlayer }: { mediaPlayer: MediaPlayer }) {
  const playState = usePlayState();
  const isPlaying = playState.value === "play";
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [isMembersModalOpen, setIsMembersModalOpen] = useState(false);

  async function handleFileLoad(files: FileList) {
    /**
     * Follow this function definition to see how we update
     * values in Jazz and manage files!
     */
    await uploadMusicTracks(files);
  }

  const params = useParams<{ playlistId: string }>();
  const playlistId = useAccountSelector({
    select: (me) =>
      params.playlistId ??
      (me.$isLoaded ? me.root.$jazz.refs.rootPlaylist.id : undefined),
  });

  const playlist = useCoState(Playlist, playlistId, {
    resolve: {
      tracks: {
        $each: true,
      },
    },
    select: (playlist) => (playlist.$isLoaded ? playlist : undefined),
  });

  const membersIds = playlist?.$jazz.owner.members.map((member) => member.id);
  const isRootPlaylist = !params.playlistId;
  const canEdit = useAccountSelector({
    select: (me) => Boolean(playlist && me.canWrite(playlist)),
  });
  const isActivePlaylist = useAccountSelector({
    select: (me) =>
      me.$isLoaded && playlistId === me.root.activePlaylist?.$jazz.id,
  });

  const handlePlaylistShareClick = () => {
    setIsMembersModalOpen(true);
  };

  const handleEditClick = () => {
    setIsEditModalOpen(true);
  };

  return (
    <SidebarInset className="flex flex-col h-screen text-gray-800">
      <div className="flex flex-1 overflow-hidden">
        <SidePanel />
        <main className="flex-1 px-2 py-4 md:px-6 overflow-y-auto overflow-x-hidden relative sm:h-[calc(100vh-80px)] bg-white h-[calc(100vh-165px)]">
          <SidebarTrigger className="md:hidden" />

          <div className="flex flex-row items-center justify-between mb-4 pl-1 md:pl-10 pr-2 md:pr-0 mt-2 md:mt-0 w-full">
            {isRootPlaylist ? (
              <h1 className="text-2xl font-bold text-blue-800">All tracks</h1>
            ) : (
              <div className="flex items-center space-x-4">
                <h1 className="text-2xl font-bold text-blue-800">
                  {playlist?.title}
                </h1>
                {membersIds && playlist && (
                  <PlaylistMembers
                    memberIds={membersIds}
                    onClick={() => setIsMembersModalOpen(true)}
                  />
                )}
              </div>
            )}
            <div className="flex items-center space-x-4">
              {isRootPlaylist && (
                <>
                  <FileUploadButton onFileLoad={handleFileLoad}>
                    Add file
                  </FileUploadButton>
                </>
              )}
              {!isRootPlaylist && canEdit && (
                <>
                  <Button onClick={handleEditClick} variant="outline">
                    Edit
                  </Button>
                  <Button onClick={handlePlaylistShareClick}>Share</Button>
                </>
              )}
            </div>
          </div>
          <ul className="flex flex-col max-w-full sm:gap-1">
            {playlist?.tracks?.map(
              (track, index) =>
                track && (
                  <MusicTrackRow
                    trackId={track.$jazz.id}
                    key={track.$jazz.id}
                    index={index}
                    isPlaying={
                      mediaPlayer.activeTrackId === track.$jazz.id &&
                      isActivePlaylist &&
                      isPlaying
                    }
                    onClick={() => {
                      mediaPlayer.setActiveTrack(track, playlist);
                    }}
                  />
                ),
            )}
          </ul>
        </main>
        <PlayerControls mediaPlayer={mediaPlayer} />
      </div>

      {/* Playlist Title Edit Modal */}
      <EditPlaylistModal
        playlistId={playlistId}
        isOpen={isEditModalOpen}
        onClose={() => setIsEditModalOpen(false)}
      />

      {/* Members Management Modal */}
      {playlist && (
        <MemberAccessModal
          isOpen={isMembersModalOpen}
          onOpenChange={setIsMembersModalOpen}
          playlist={playlist}
        />
      )}
    </SidebarInset>
  );
}
