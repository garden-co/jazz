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
  ClipboardCheckIcon,
  ClipboardIcon,
  CodeIcon,
  Eye,
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
  SearchIcon,
  SunIcon,
  TrashIcon,
  UploadCloudIcon,
  UserIcon,
  UsersIcon,
  WifiOffIcon,
  XIcon,
  QrCodeIcon,
  ServerIcon,
} from "lucide-react";

import clsx from "clsx";
import {
  Style,
  styleToTextHoverMap,
  styleToTextMap,
} from "../../utils/tailwindClassesMap";
import { GcmpIcons } from "./icons";

export const icons = {
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
  copySuccess: ClipboardCheckIcon,
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
  search: SearchIcon,
  qrcode: QrCodeIcon,
  previous: ChevronLeftIcon,
  next: ChevronRightIcon,
  server: ServerIcon,
  // text editor icons
  bold: BoldIcon,
  italic: ItalicIcon,
  eye: Eye,
};

// copied from tailwind line height https://tailwindcss.com/docs/font-size
const sizes = {
  "2xs": 14,
  xs: 16,
  sm: 20,
  md: 22,
  lg: 26,
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
  intent = "default",
  hasBackground = false,
  className,
  hasHover = false,
  ...svgProps
}: {
  name?: IconName;
  icon?: LucideIcon;
  size?: keyof typeof sizes;
  intent?: Style | "white";
  hasBackground?: boolean;
  className?: string;
  hasHover?: boolean;
} & React.SVGProps<SVGSVGElement>) {
  if (!icon && (!name || !icons.hasOwnProperty(name))) {
    throw new Error(`Icon not found: ${name}`);
  }

  // @ts-ignore
  const IconComponent = icons?.hasOwnProperty(name) ? icons[name] : icon;

  const iconClass = {
    ...styleToTextMap,
    white: "text-white",
  };

  const iconHoverClass = {
    ...styleToTextHoverMap,
    white: "hover:text-white/90",
  };

  const backgroundClasses = {
    default: "bg-stone-200/30 dark:bg-stone-900/30",
    primary: "bg-primary-transparent",
    secondary: "bg-secondary-transparent",
    info: "bg-info-transparent",
    success: "bg-success-transparent",
    warning: "bg-warning-transparent",
    danger: "bg-danger-transparent",
    alert: "bg-alert-transparent",
    tip: "bg-tip-transparent",
    muted: "bg-stone-300/30 dark:bg-stone-700/30",
    strong: "bg-stone-900/30 dark:bg-stone-100/30",
  };

  const roundedClasses = {
    xs: "rounded-xs",
    sm: "rounded-sm",
    md: "rounded-md",
    lg: "rounded-lg",
    xl: "rounded-xl",
  };

  return (
    <IconComponent
      aria-hidden="true"
      size={sizes[size]}
      strokeWidth={strokeWidths[size]}
      strokeLinecap="round"
      className={clsx(
        roundedClasses[size as keyof typeof roundedClasses] || "rounded-lg",
        iconClass[intent as keyof typeof iconClass],
        hasBackground &&
          backgroundClasses[intent as keyof typeof backgroundClasses],
        hasHover && iconHoverClass[intent as keyof typeof iconHoverClass],
        className,
      )}
      {...svgProps}
    />
  );
}
