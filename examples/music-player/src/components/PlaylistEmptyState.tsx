import { Button } from "./ui/button";

interface PlaylistEmptyStateProps {
  canEdit: boolean;
  onAddTracks: () => void;
}

export function PlaylistEmptyState({
  canEdit,
  onAddTracks,
}: PlaylistEmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-16 px-4 text-center">
      <div className="max-w-md">
        <h2 className="text-xl font-semibold text-gray-800 mb-2">
          This playlist is empty
        </h2>
        <p className="text-gray-600 mb-6">
          Add tracks from your library to get started.
        </p>
        {canEdit && (
          <Button
            onClick={onAddTracks}
            className="bg-blue-600 hover:bg-blue-700"
          >
            Add tracks
          </Button>
        )}
      </div>
    </div>
  );
}
