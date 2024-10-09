import { JazzLogo } from "./logos";
import { Nav } from "gcmp-design-system/src/app/components/organisms/Nav";
import { SiDiscord, SiGithub, SiTwitter } from "@icons-pack/react-simple-icons";
import { DocNav } from "./docs/nav";

export function JazzNav() {
    return (
        <Nav
            mainLogo={<JazzLogo className="w-24 -ml-2" />}
            items={[
                { title: "Home", href: "/" },
                { title: "Jazz Mesh", href: "/mesh" },
                {
                    title: "Docs",
                    href: "/docs",
                },
                {
                    title: "Showcase",
                    href: "/showcase",
                },
                {
                    title: "Blog",
                    href: "https://gcmp.io/news",
                    firstOnRight: true,
                    newTab: true,
                },
                {
                    title: "Releases",
                    href: "https://github.com/gardencmp/jazz/releases",
                    newTab: true,
                },
                {
                    title: "Roadmap",
                    href: "https://github.com/orgs/gardencmp/projects/4/views/3",
                    newTab: true,
                },
                {
                    title: "GitHub",
                    href: "https://github.com/gardencmp/jazz",
                    newTab: true,
                    icon: <SiGithub className="w-5" />,
                },
                {
                    title: "Discord",
                    href: "https://discord.gg/utDMjHYg42",
                    newTab: true,
                    icon: <SiDiscord className="w-5" />,
                },
                {
                    title: "X",
                    href: "https://x.com/jazz_tools",
                    newTab: true,
                    icon: <SiTwitter className="w-5" />,
                },
            ]}
            docNav={<DocNav />}
        />
    );
}
