import { Badge } from "@/components/ui/badge";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useIsAnonymousUser } from "jazz-react";
import { Info } from "lucide-react";

export function LocalOnlyTag() {
  const isAnonymousUser = useIsAnonymousUser();

  if (!isAnonymousUser) {
    return null;
  }

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="inline-flex items-center gap-1.5 cursor-help">
            <Badge variant="default" className="h-5 text-xs font-normal">
              Local only
            </Badge>
            <Info className="h-3.5 w-3.5 text-muted-foreground" />
          </div>
        </TooltipTrigger>
        <TooltipContent className="max-w-[250px]">
          <p>
            Sign up to enable network sync and share your playlists with others
          </p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
