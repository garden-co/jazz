import { useAccount } from "jazz-react";
import { Button } from "./ui/button";

export function LogoutButton() {
  const { logOut } = useAccount();

  return <Button onClick={logOut}>Logout</Button>;
}
