import { MusicaAccount } from "@/1_schema";
import { deletePlaylist } from "@/4_actions";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuAction,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { useAccountWithSelector } from "jazz-tools/react";
import { Home, Music, Plus, Trash2 } from "lucide-react";
import { useNavigate, useParams } from "react-router";
import { useState } from "react";
import { AuthButton } from "./AuthButton";
import { CreatePlaylistModal } from "./CreatePlaylistModal";

export function SidePanel() {
  const { playlistId } = useParams();
  const navigate = useNavigate();
  const playlists = useAccountWithSelector(MusicaAccount, {
    resolve: { root: { playlists: { $each: { $onError: null } } } },
    select: (me) => me?.root.playlists,
  });
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);

  function handleAllTracksClick() {
    navigate(`/`);
  }

  function handlePlaylistClick(playlistId: string) {
    navigate(`/playlist/${playlistId}`);
  }

  async function handleDeletePlaylist(playlistId: string) {
    if (confirm("Are you sure you want to delete this playlist?")) {
      await deletePlaylist(playlistId);
      navigate(`/`);
    }
  }

  function handleCreatePlaylistClick() {
    setIsCreateModalOpen(true);
  }

  function handlePlaylistCreated(playlistId: string) {
    navigate(`/playlist/${playlistId}`);
  }

  return (
    <>
      <Sidebar>
        <SidebarHeader>
          <SidebarMenu>
            <SidebarMenuItem className="flex items-center gap-2">
              <div className="flex aspect-square size-8 items-center justify-center rounded-lg bg-blue-600 text-white">
                <svg
                  className="size-4"
                  viewBox="0 0 24 24"
                  fill="none"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  <path
                    d="M9 18V5l12-2v13"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                  <path
                    d="M6 15H3c-1.1 0-2 .9-2 2v4c0 1.1.9 2 2 2h3c1.1 0 2-.9 2-2v-4c0-1.1-.9-2-2-2zM18 13h-3c-1.1 0-2 .9-2 2v4c0 1.1.9 2 2 2h3c1.1 0 2-.9 2-2v-4c0-1.1-.9-2-2-2z"
                    fill="currentColor"
                  />
                </svg>
              </div>
              <div className="grid flex-1 text-left text-sm leading-tight">
                <span className="truncate font-semibold">Music Player</span>
              </div>
              <AuthButton />
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarHeader>
        <SidebarContent>
          <SidebarGroup>
            <SidebarGroupContent>
              <SidebarMenu>
                <SidebarMenuItem>
                  <SidebarMenuButton onClick={handleAllTracksClick}>
                    <Home className="size-4" />
                    <span>Go to all tracks</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
          <SidebarGroup>
            <SidebarGroupLabel>Playlists</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                <SidebarMenuItem>
                  <SidebarMenuButton onClick={handleCreatePlaylistClick}>
                    <Plus className="size-4" />
                    <span>Add a new playlist</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                {playlists?.map(
                  (playlist) =>
                    playlist && (
                      <SidebarMenuItem key={playlist.$jazz.id}>
                        <SidebarMenuButton
                          onClick={() => handlePlaylistClick(playlist.$jazz.id)}
                          isActive={playlist.$jazz.id === playlistId}
                        >
                          <div className="flex items-center gap-2">
                            <Music className="size-4" />
                            <span>{playlist.title}</span>
                          </div>
                        </SidebarMenuButton>
                        {playlist.$jazz.id === playlistId && (
                          <SidebarMenuAction
                            onClick={() =>
                              handleDeletePlaylist(playlist.$jazz.id)
                            }
                            className="text-red-600 hover:text-red-800"
                          >
                            <Trash2 className="size-4" />
                            <span className="sr-only">
                              Delete {playlist.title}
                            </span>
                          </SidebarMenuAction>
                        )}
                      </SidebarMenuItem>
                    ),
                )}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
      </Sidebar>

      {/* Create Playlist Modal */}
      <CreatePlaylistModal
        isOpen={isCreateModalOpen}
        onClose={() => setIsCreateModalOpen(false)}
        onPlaylistCreated={handlePlaylistCreated}
      />
    </>
  );
}
