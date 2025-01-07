import { HowToPlayContent } from "@/components/how-to-play-content";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Link, createFileRoute } from "@tanstack/react-router";
import { ArrowLeftIcon } from "lucide-react";

export const Route = createFileRoute("/how-to-play")({
  component: RouteComponent,
});

function RouteComponent() {
  return (
    <div className="flex flex-col w-full place-items-center justify-center p-2 min-h-screen">
      <Card className="max-w-screen-lg">
        <CardHeader>
          <CardTitle>How to Play?</CardTitle>
        </CardHeader>
        <CardContent>
          <HowToPlayContent />
        </CardContent>
        <CardFooter className="flex justify-end">
          <Button asChild variant="link">
            <Link to="/">
              <ArrowLeftIcon className="w-5 h-5" />
              Play
            </Link>
          </Button>
        </CardFooter>
      </Card>
    </div>
  );
}
