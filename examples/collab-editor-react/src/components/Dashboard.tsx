import { useState } from "react";
import { toast } from "sonner";
import { useCreateRoom } from "../hooks/useCreateRoom.js";
import { useRooms } from "../hooks/useRooms.js";
import { navigate } from "../hooks/useRouter.js";
import { getDisplayName } from "../lib/identity.js";

export function Dashboard() {
  const rooms = useRooms();
  const createRoom = useCreateRoom();
  const [isCreating, setIsCreating] = useState(false);

  const handleCreateRoom = async () => {
    setIsCreating(true);
    try {
      const shareToken = await createRoom();
      navigate(`/r/${shareToken}`);
    } catch (error) {
      console.error("Failed to create room", error);
      toast.error("Could not create a room");
    } finally {
      setIsCreating(false);
    }
  };

  return (
    <main>
      <header>
        <p>you are {getDisplayName()}</p>
        <h1>Collaborative editor</h1>
        <button
          type="button"
          data-testid="new-room"
          onClick={handleCreateRoom}
          disabled={isCreating}
        >
          New room
        </button>
      </header>

      <section>
        <h2>Your rooms</h2>
        <ul id="room-list">
          {rooms.map((room) => (
            <li key={room.id}>
              <a className="room-link" href={`/r/${room.shareToken}`}>
                {room.title}
              </a>
            </li>
          ))}
        </ul>
      </section>
    </main>
  );
}
