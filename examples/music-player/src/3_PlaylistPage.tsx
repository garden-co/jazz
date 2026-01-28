import { useParams } from "react-router";
import { MusicaAccount, PlaylistWithTracks } from "./1_schema";
import { uploadMusicTracks } from "./4_actions";
import { MediaPlayer } from "./5_useMediaPlayer";
import { FileUploadButton } from "./components/FileUploadButton";
import { MusicTrackRow } from "./components/MusicTrackRow";
import { PlayerControls } from "./components/PlayerControls";
import { PlaylistMembers } from "./components/PlaylistMembers";
import { EditPlaylistDialog } from "./components/EditPlaylistDialog";
import { AddTracksDialog } from "./components/AddTracksDialog";
import { PlaylistEmptyState } from "./components/PlaylistEmptyState";
import { SidePanel } from "./components/SidePanel";
import { Button } from "./components/ui/button";
import { SidebarInset, SidebarTrigger } from "./components/ui/sidebar";
import { useState, useSyncExternalStore } from "react";
import { useSuspenseAccount, useSuspenseCoState } from "jazz-tools/react-core";
import { useIsMobile } from "./hooks/use-mobile";
import { Pencil } from "lucide-react";
import { useAudioManager } from "./lib/audio/AudioManager";

export function PlaylistPage({ mediaPlayer }: { mediaPlayer: MediaPlayer }) {
  const params = useParams<{ playlistId: string }>();
  const playlistId = useSuspenseAccount(MusicaAccount, {
    select: (me) => params.playlistId ?? me.root.$jazz.refs.rootPlaylist.id,
  });
  const isMobile = useIsMobile();

  const playlist = useSuspenseCoState(PlaylistWithTracks, playlistId);

  const membersIds = playlist.$jazz.owner.members.map((member) => member.id);
  const isRootPlaylist = !params.playlistId;
  const canEdit = useSuspenseAccount(MusicaAccount, {
    select: (me) => me.canWrite(playlist),
  });

  const canManage = useSuspenseAccount(MusicaAccount, {
    select: (me) => me.canManage(playlist),
  });

  const audioManager = useAudioManager();
  const isPlaying = useSyncExternalStore(
    (callback) => audioManager.on("statusChange", callback),
    () => audioManager.isPlaying,
  );
  const [currentDialog, setCurrentDialog] = useState<
    | { name: "playlist"; section: "details" | "members" }
    | { name: "add-tracks" }
    | null
  >(null);

  async function handleFileLoad(files: FileList) {
    /**
     * Follow this function definition to see how we update
     * values in Jazz and manage files!
     */
    await uploadMusicTracks(playlist, files);
  }

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
              <div className="group flex items-center gap-3">
                <div className="flex items-center gap-1">
                  <h1 className="text-2xl font-bold text-blue-800">
                    {canEdit ? (
                      <button
                        type="button"
                        onClick={() =>
                          setCurrentDialog({
                            name: "playlist",
                            section: "details",
                          })
                        }
                        className="text-left hover:underline focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 rounded-sm"
                        aria-label="Edit playlist title"
                      >
                        {playlist.title}
                      </button>
                    ) : (
                      playlist.title
                    )}
                  </h1>
                  {canEdit && (
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      aria-label="Edit playlist"
                      onClick={() =>
                        setCurrentDialog({
                          name: "playlist",
                          section: "details",
                        })
                      }
                      className="text-blue-800"
                    >
                      <Pencil className="w-4 h-4" />
                    </Button>
                  )}
                </div>
                <PlaylistMembers
                  memberIds={membersIds}
                  onClick={() =>
                    setCurrentDialog({ name: "playlist", section: "members" })
                  }
                />
              </div>
            )}
            <div className="flex items-center space-x-4">
              {isRootPlaylist ? (
                <>
                  <FileUploadButton onFileLoad={handleFileLoad}>
                    Add file
                  </FileUploadButton>
                </>
              ) : (
                <>
                  {canEdit && (
                    <Button
                      onClick={() => setCurrentDialog({ name: "add-tracks" })}
                      variant="outline"
                    >
                      {isMobile ? "Add tracks" : "Add tracks from library"}
                    </Button>
                  )}
                  {canManage && (
                    <Button
                      onClick={() =>
                        setCurrentDialog({
                          name: "playlist",
                          section: "members",
                        })
                      }
                    >
                      Share
                    </Button>
                  )}
                </>
              )}
            </div>
          </div>
          {playlist.tracks.length > 0 ? (
            <ul className="flex flex-col max-w-full sm:gap-1">
              {playlist.tracks.map(
                (track, index) =>
                  track && (
                    <MusicTrackRow
                      trackId={track.$jazz.id}
                      key={track.$jazz.id}
                      index={index}
                      isPlaying={
                        isPlaying &&
                        mediaPlayer.activeTrackId === track.$jazz.id
                      }
                      isLoading={mediaPlayer.loading === track.$jazz.id}
                      onClick={() => {
                        mediaPlayer.setActiveTrack(track, playlist);
                      }}
                    />
                  ),
              )}
            </ul>
          ) : (
            !isRootPlaylist && (
              <PlaylistEmptyState
                canEdit={canEdit}
                onAddTracks={() => setCurrentDialog({ name: "add-tracks" })}
              />
            )
          )}
        </main>
        <PlayerControls mediaPlayer={mediaPlayer} />
      </div>

      {/* Playlist Edit / Members Dialog */}
      {currentDialog?.name === "playlist" && (
        <EditPlaylistDialog
          isOpen={true}
          onOpenChange={(open) => !open && setCurrentDialog(null)}
          playlistId={playlistId}
          defaultSection={currentDialog.section}
        />
      )}
      {/* Add Tracks from Root Modal */}
      {currentDialog?.name === "add-tracks" && (
        <AddTracksDialog
          isOpen={true}
          onOpenChange={(open) => !open && setCurrentDialog(null)}
          playlist={playlist}
        />
      )}
    </SidebarInset>
  );
}
