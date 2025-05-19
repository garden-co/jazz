import {
  AlertTriangleIcon,
  ArrowDownIcon,
  ArrowRightIcon,
  BoldIcon,
  BookTextIcon,
  BoxIcon,
  BracesIcon,
  Brackets,
  CheckIcon,
  ChevronDown,
  ChevronLeftIcon,
  ChevronRight,
  ChevronRightIcon,
  ClipboardIcon,
  CodeIcon,
  FileLock2Icon,
  FileTextIcon,
  FingerprintIcon,
  FolderArchiveIcon,
  GaugeIcon,
  GlobeIcon,
  HashIcon,
  ImageIcon,
  InfoIcon,
  ItalicIcon,
  LinkIcon,
  LockKeyholeIcon,
  type LucideIcon,
  MailIcon,
  MenuIcon,
  MessageCircleQuestionIcon,
  MonitorSmartphoneIcon,
  MoonIcon,
  MousePointer2Icon,
  MousePointerSquareDashedIcon,
  ScanFace,
  ScrollIcon,
  SunIcon,
  TrashIcon,
  UploadCloudIcon,
  UserIcon,
  UsersIcon,
  WifiOffIcon,
  XIcon,
} from "lucide-react";

import { GcmpIcons } from "./icons";

const icons = {
  arrowDown: ArrowDownIcon,
  arrowRight: ArrowRightIcon,
  auth: UserIcon,
  browser: GlobeIcon,
  check: CheckIcon,
  chevronRight: ChevronRight,
  chevronDown: ChevronDown,
  close: XIcon,
  code: CodeIcon,
  copy: ClipboardIcon,
  cursor: MousePointer2Icon,
  darkTheme: MoonIcon,
  delete: TrashIcon,
  devices: MonitorSmartphoneIcon,
  docs: BookTextIcon,
  encryption: LockKeyholeIcon,
  faceId: ScanFace,
  file: FileTextIcon,
  hash: HashIcon,
  help: MessageCircleQuestionIcon,
  image: ImageIcon,
  instant: GaugeIcon,
  lightTheme: SunIcon,
  link: LinkIcon,
  menu: MenuIcon,
  newsletter: MailIcon,
  offline: WifiOffIcon,
  package: BoxIcon,
  permissions: FileLock2Icon,
  social: UsersIcon,
  spatialPresence: MousePointerSquareDashedIcon,
  tableOfContents: ScrollIcon,
  touchId: FingerprintIcon,
  upload: UploadCloudIcon,
  zip: FolderArchiveIcon,
  warning: AlertTriangleIcon,
  info: InfoIcon,
  cofeed: GcmpIcons.IconCoFeed,
  corecord: GcmpIcons.IconCoRecord,
  comap: BracesIcon,
  colist: Brackets,
  user: UserIcon,
  group: UsersIcon,

  previous: ChevronLeftIcon,
  next: ChevronRightIcon,

  // text editor icons
  bold: BoldIcon,
  italic: ItalicIcon,
};

// copied from tailwind line height https://tailwindcss.com/docs/font-size
const sizes = {
  "2xs": 14,
  xs: 16,
  sm: 20,
  md: 24,
  lg: 28,
  xl: 28,
  "2xl": 32,
  "3xl": 36,
  "4xl": 40,
  "5xl": 48,
  "6xl": 60,
  "7xl": 72,
  "8xl": 96,
  "9xl": 128,
};

const strokeWidths = {
  "2xs": 2.5,
  xs: 2,
  sm: 2,
  md: 1.5,
  lg: 1.5,
  xl: 1.5,
  "2xl": 1.25,
  "3xl": 1.25,
  "4xl": 1.25,
  "5xl": 1,
  "6xl": 1,
  "7xl": 1,
  "8xl": 1,
  "9xl": 1,
};

export type IconName = keyof typeof icons;

export function Icon({
  name,
  icon,
  size = "md",
  className,
  ...svgProps
}: {
  name?: IconName;
  icon?: LucideIcon;
  size?: keyof typeof sizes;
  className?: string;
} & React.SVGProps<SVGSVGElement>) {
  if (!icon && (!name || !icons.hasOwnProperty(name))) {
    throw new Error(`Icon not found: ${name}`);
  }

  // @ts-ignore
  const IconComponent = icons?.hasOwnProperty(name) ? icons[name] : icon;

  return (
    <IconComponent
      aria-hidden="true"
      size={sizes[size]}
      strokeWidth={strokeWidths[size]}
      strokeLinecap="round"
      className={className}
      {...svgProps}
    />
  );
}
