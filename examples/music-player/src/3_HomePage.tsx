import { useToast } from "@/hooks/use-toast";
import {
  createInviteLink,
  useAccount,
  useCoState,
  useIsAuthenticated,
} from "jazz-react";
import { useParams } from "react-router";
import { MusicaAccount, Playlist } from "./1_schema";
import { uploadMusicTracks } from "./4_actions";
import { MediaPlayer } from "./5_useMediaPlayer";
import { FileUploadButton } from "./components/FileUploadButton";
import { MusicTrackRow } from "./components/MusicTrackRow";
import { PlaylistTitleInput } from "./components/PlaylistTitleInput";
import { SidePanel } from "./components/SidePanel";
import { Button } from "./components/ui/button";
import { SidebarInset, SidebarTrigger } from "./components/ui/sidebar";
import { usePlayState } from "./lib/audio/usePlayState";

export function HomePage({ mediaPlayer }: { mediaPlayer: MediaPlayer }) {
  /**
   * `me` represents the current user account, which will determine
   *  access rights to CoValues. We get it from the top-level provider `<WithJazz/>`.
   */
  const { me } = useAccount(MusicaAccount, {
    resolve: { root: { rootPlaylist: true, playlists: true } },
  });

  const playState = usePlayState();
  const isPlaying = playState.value === "play";
  const { toast } = useToast();

  async function handleFileLoad(files: FileList) {
    /**
     * Follow this function definition to see how we update
     * values in Jazz and manage files!
     */
    await uploadMusicTracks(files);
  }

  const params = useParams<{ playlistId: string }>();
  const playlistId = params.playlistId ?? me?.root._refs.rootPlaylist.id;

  const playlist = useCoState(Playlist, playlistId, {
    resolve: { tracks: true },
  });

  const isRootPlaylist = !params.playlistId;
  const isPlaylistOwner = playlist?._owner.myRole() === "admin";
  const isActivePlaylist = playlistId === me?.root.activePlaylist?.id;

  const handlePlaylistShareClick = async () => {
    if (!isPlaylistOwner) return;

    const inviteLink = createInviteLink(playlist, "reader");

    await navigator.clipboard.writeText(inviteLink);

    toast({
      title: "Invite link copied into the clipboard",
    });
  };

  const isAuthenticated = useIsAuthenticated();

  return (
    <SidebarInset className="flex flex-col h-screen text-gray-800 bg-blue-50">
      <div className="flex flex-1 overflow-hidden">
        <SidePanel mediaPlayer={mediaPlayer} />
        <main className="flex-1 p-6 overflow-y-auto overflow-x-hidden">
          <SidebarTrigger />
          <div className="flex items-center justify-between mb-6">
            {isRootPlaylist ? (
              <h1 className="text-2xl font-bold text-blue-800">All tracks</h1>
            ) : (
              <PlaylistTitleInput playlistId={playlistId} />
            )}
            <div className="flex items-center space-x-4">
              {isRootPlaylist && (
                <>
                  <FileUploadButton onFileLoad={handleFileLoad}>
                    Add file
                  </FileUploadButton>
                </>
              )}
              {!isRootPlaylist && isAuthenticated && (
                <Button onClick={handlePlaylistShareClick}>
                  Share playlist
                </Button>
              )}
            </div>
          </div>
          <ul className="flex flex-col max-w-full">
            {playlist?.tracks?.map(
              (track) =>
                track && (
                  <MusicTrackRow
                    trackId={track.id}
                    key={track.id}
                    isLoading={mediaPlayer.loading === track.id}
                    isPlaying={
                      mediaPlayer.activeTrackId === track.id &&
                      isActivePlaylist &&
                      isPlaying
                    }
                    onClick={() => {
                      mediaPlayer.setActiveTrack(track, playlist);
                    }}
                    showAddToPlaylist={isRootPlaylist}
                  />
                ),
            )}
          </ul>
        </main>
      </div>
    </SidebarInset>
  );
}
