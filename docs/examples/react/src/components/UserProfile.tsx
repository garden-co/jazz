import { useOne, useMutate } from "@jazz/react";
import { app } from "../generated/client.js";

//#region user-profile
interface UserProfileProps {
  userId: string;
}

export function UserProfile({ userId }: UserProfileProps) {
  const [user, loading, mutate] = useOne(app.users, userId);

  if (loading) return <div>Loading...</div>;
  if (!user) return <div>User not found</div>;

  return (
    <div>
      <h1>{user.name}</h1>
      <p>Email: {user.email}</p>
      <button onClick={() => mutate.update({ name: "New Name" })}>
        Rename
      </button>
      <button onClick={() => mutate.delete()}>
        Delete
      </button>
    </div>
  );
}
//#endregion

//#region create-user-button
export function CreateUserButton() {
  const mutate = useMutate(app.users);

  return (
    <button onClick={() => {
      const id = mutate.create({
        name: "New User",
        email: "user@example.com",
        age: BigInt(25),
        score: 0.0,
        isAdmin: false,
      });
      console.log("Created user with id:", id);
    }}>
      Create User
    </button>
  );
}
//#endregion
