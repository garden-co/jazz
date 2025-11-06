import { useLogOut } from "jazz-tools/react";
import { Button } from "./ui/button";

export function LogoutButton() {
  const logOut = useLogOut();

  return <Button onClick={logOut}>Logout</Button>;
}
