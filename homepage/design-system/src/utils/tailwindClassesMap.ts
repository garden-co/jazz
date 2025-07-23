export type Variant =
  | "default"
  | "secondary"
  | "destructive"
  | "ghost"
  | "outline"
  | "link"
  | "inverted"
  | "glass";

export type Style =
  | "default"
  | "primary"
  | "tip"
  | "info"
  | "success"
  | "warning"
  | "alert"
  | "danger"
  | "muted"
  | "strong";

export const sizeClasses = {
  sm: "text-sm py-1 px-2",
  md: "py-1.5 px-3 h-[36px]",
  lg: "py-2 px-5 md:px-6 md:py-2.5",
};

export const styleToBorderMap = {
  primary: "border-primary",
  info: "border-info",
  success: "border-success",
  warning: "border-warning",
  danger: "border-danger",
  alert: "border-alert",
  tip: "border-tip",
  muted: "border-stone-200 dark:border-stone-700",
  strong: "border-stone-900 dark:border-stone-100",
  default: "border-stone-600 dark:border-stone-200",
};

export const styleToActiveBorderMap = {
  primary: "active:border-primary-transparent focus:border-primary-transparent",
  info: "active:border-info-transparent focus:border-info-transparent",
  success: "active:border-success-transparent focus:border-success-transparent",
  warning: "active:border-warning-transparent focus:border-warning-transparent",
  danger: "active:border-danger-transparent focus:border-danger-transparent",
  alert: "active:border-alert-transparent focus:border-alert-transparent",
  tip: "active:border-tip-transparent focus:border-tip-transparent",
  muted:
    "active:border-stone-200/30 focus:border-stone-200/30 dark:active:border-stone-900/30 dark:focus:border-stone-900/30",
  strong:
    "active:border-stone-900/30 focus:border-stone-900/30 dark:active:border-stone-200/30 dark:focus:border-stone-200/30",
  default:
    "active:border-stone-600/30 dark:active:border-stone-100/30 focus:border-stone-600/30 dark:focus:border-stone-100/30",
};

export const styleToBgMap = {
  primary: "bg-primary",
  info: "bg-info",
  success: "bg-success",
  warning: "bg-warning",
  danger: "bg-danger",
  alert: "bg-alert",
  tip: "bg-tip",
  muted: "bg-stone-200 dark:bg-stone-900",
  strong: "bg-stone-900 dark:bg-stone-200",
  default: "bg-stone-700 dark:bg-stone-100",
};

export const styleToBgTransparentHoverMap = {
  primary: "hover:bg-primary-transparent",
  info: "hover:bg-info-transparent",
  success: "hover:bg-success-transparent",
  warning: "hover:bg-warning-transparent",
  danger: "hover:bg-danger-transparent",
  alert: "hover:bg-alert-transparent",
  tip: "hover:bg-tip-transparent",
  muted: "hover:bg-stone-100/20 dark:hover:bg-stone-900/20",
  strong: "hover:bg-stone-900/20 dark:hover:bg-stone-100/20",
  default: "hover:bg-stone-600/20 dark:hover:bg-stone-100/20",
};

export const styleToBgTransparentActiveMap = {
  primary: "active:bg-blue/20",
  info: "active:bg-purple/20",
  success: "active:bg-green/20",
  warning: "active:bg-orange/20",
  danger: "active:bg-red/20",
  alert: "active:bg-yellow/20",
  tip: "active:bg-cyan/20",
  muted: "active:bg-stone-400/20",
  strong: "active:bg-stone-900/20",
  default: "active:bg-stone-600/20 dark:active:bg-stone-100/20",
};

export const styleToTextMap = {
  primary: "text-primary",
  info: "text-info",
  success: "text-success",
  warning: "text-warning",
  danger: "text-danger",
  alert: "text-alert",
  tip: "text-tip",
  muted: "text-stone-500 dark:text-stone-400",
  strong: "text-stone-900 dark:text-white",
  default: "text-stone-700 dark:text-stone-100",
};

export const styleToTextHoverMap = {
  primary: "hover:text-primary-light",
  info: "hover:text-info-light",
  success: "hover:text-success-light",
  warning: "hover:text-warning-light",
  danger: "hover:text-danger-light",
  alert: "hover:text-alert-light",
  tip: "hover:text-tip-light",
  muted: "hover:text-stone-400 dark:hover:text-stone-500",
  strong: "hover:text-stone-700 dark:hover:text-stone-300",
  default: "hover:text-stone-600 dark:hover:text-stone-200",
};

export const styleToTextActiveMap = {
  primary: "active:text-primary-dark",
  info: "active:text-info-dark",
  success: "active:text-success-dark",
  warning: "active:text-warning-dark",
  danger: "active:text-danger-dark",
  alert: "active:text-alert-dark",
  tip: "active:text-tip-dark",
  muted: "active:text-stone-400 dark:active:text-stone-500",
  strong: "active:text-stone-700 dark:active:text-stone-300",
  default: "active:text-stone-800 dark:active:text-stone-400",
};

export type VariantColor =
  | "blue"
  | "indigo"
  | "purple"
  | "green"
  | "orange"
  | "red"
  | "yellow"
  | "cyan"
  | "muted"
  | "strong"
  | "default";

export const styleToColorMap = {
  primary: "blue",
  info: "purple",
  success: "green",
  warning: "orange",
  danger: "red",
  alert: "yellow",
  tip: "cyan",
  muted: "muted",
  strong: "strong",
  default: "default",
};

export const colorToBgMap = {
  blue: "bg-blue",
  indigo: "bg-indigo-500",
  purple: "bg-purple",
  green: "bg-green",
  orange: "bg-orange",
  red: "bg-red",
  yellow: "bg-yellow",
  cyan: "bg-cyan",
  muted: "bg-stone-200 dark:bg-stone-900",
  strong: "bg-stone-900 dark:bg-stone-100",
  default: "bg-stone-600 dark:bg-white",
};

export const colorToBgMap20 = {
  blue: "bg-blue/20",
  indigo: "bg-indigo-500/20",
  purple: "bg-purple/20",
  green: "bg-green/20",
  orange: "bg-orange/20",
  red: "bg-red/20",
  yellow: "bg-yellow/20",
  cyan: "bg-cyan/20",
  muted: "bg-stone-200/20 dark:bg-stone-900/50",
  strong: "bg-stone-900/20 dark:bg-stone-100/50",
  default: "bg-stone-600/20 dark:bg-white/20",
};

export const colorToBgHoverMap30 = {
  blue: "hover:bg-blue/30",
  indigo: "hover:bg-indigo-500/30",
  purple: "hover:bg-purple/30",
  green: "hover:bg-green/30",
  orange: "hover:bg-orange/30",
  red: "hover:bg-red/30",
  yellow: "hover:bg-yellow/30",
  cyan: "hover:bg-cyan/30",
  muted: "hover:bg-stone-200/30 dark:hover:bg-stone-900/30",
  strong: "hover:bg-stone-900/30 dark:hover:bg-stone-100/30",
  default: "hover:bg-stone-600/30 dark:hover:bg-white/30",
};

export const colorToBgHoverMap10 = {
  blue: "hover:bg-blue/10",
  indigo: "hover:bg-indigo-500/10",
  purple: "hover:bg-purple/10",
  green: "hover:bg-green/10",
  orange: "hover:bg-orange/10",
  red: "hover:bg-red/10",
  yellow: "hover:bg-yellow/10",
  cyan: "hover:bg-cyan/10",
  muted: "hover:bg-stone-200/30 dark:hover:bg-stone-800/30",
  strong: "hover:bg-stone-900/10 dark:hover:bg-stone-100/10",
  default: "hover:bg-stone-600/10 dark:hover:bg-white/10",
};

export const colorToBgActiveMap50 = {
  blue: "active:bg-blue/50",
  indigo: "active:bg-indigo-500/50",
  purple: "active:bg-purple/50",
  green: "active:bg-green/50",
  orange: "active:bg-orange/50",
  red: "active:bg-red/50",
  yellow: "active:bg-yellow/50",
  cyan: "active:bg-cyan/50",
  muted: "active:bg-stone-100/50 dark:active:bg-stone-900/50",
  strong: "active:bg-stone-800/40 dark:active:bg-stone-200/40",
  default: "active:bg-stone-900/40 dark:active:bg-white/50",
};

export const colorToBgActiveMap25 = {
  blue: "active:bg-blue/25",
  indigo: "active:bg-indigo-500/25",
  purple: "active:bg-purple/25",
  green: "active:bg-green/25",
  orange: "active:bg-orange/25",
  red: "active:bg-red/25",
  yellow: "active:bg-yellow/25",
  cyan: "active:bg-cyan/25",
  muted: "active:bg-stone-100/25 dark:active:bg-stone-900/25",
  strong: "active:bg-stone-900/25 dark:active:bg-stone-100/25",
  default: "active:bg-black/25 dark:active:bg-white/25",
};

const gradiantClassesBase = "bg-gradient-to-t from-7% via-50% to-95%";

export const styleToBgGradientColorMap = {
  primary: `from-primary-dark via-primary to-primary-light ${gradiantClassesBase}`,
  info: `from-info-dark via-info to-info-light ${gradiantClassesBase}`,
  success: `from-success-dark via-success to-success-light ${gradiantClassesBase}`,
  warning: `from-warning-dark via-warning to-warning-light ${gradiantClassesBase}`,
  danger: `from-danger-dark via-danger to-danger-light ${gradiantClassesBase}`,
  alert: `from-alert-dark via-alert to-alert-light ${gradiantClassesBase}`,
  tip: `from-tip-dark via-tip to-tip-light ${gradiantClassesBase}`,
  muted: `from-stone-200 via-stone-300 to-stone-400 ${gradiantClassesBase} dark:from-stone-900 dark:via-stone-900 dark:to-stone-800`,
  strong: `from-stone-700 via-stone-800 to-stone-900 ${gradiantClassesBase} dark:from-stone-100 dark:via-stone-200 dark:to-stone-300`,
  default: `from-stone-200/40 via-white to-stone-100 ${gradiantClassesBase} dark:from-stone-900 dark:via-black dark:to-stone-950`,
};

export const styleToBgGradientHoverMap = {
  primary: `hover:from-primary-brightLight hover:to-primary-light ${gradiantClassesBase}`,
  info: `hover:from-info-brightLight hover:to-info-light ${gradiantClassesBase}`,
  success: `hover:from-success-brightLight hover:to-success-light ${gradiantClassesBase}`,
  warning: `hover:from-warning-brightLight hover:to-warning-light ${gradiantClassesBase}`,
  danger: `hover:from-danger-brightLight hover:to-danger-light ${gradiantClassesBase}`,
  alert: `hover:from-alert-brightLight hover:to-alert-light ${gradiantClassesBase}`,
  tip: `hover:from-tip-brightLight hover:to-tip-light ${gradiantClassesBase}`,
  muted: `hover:from-stone-200 hover:to-stone-300 ${gradiantClassesBase} dark:hover:from-stone-900 dark:hover:to-stone-700/70`,
  strong: `hover:from-stone-700 hover:to-stone-800 ${gradiantClassesBase} dark:hover:from-stone-100 dark:hover:to-stone-200`,
  default: `hover:from-stone-100/50 hover:to-stone-100/50 dark:hover:from-stone-950 dark:hover:to-stone-900 ${gradiantClassesBase} border border-stone-100 dark:border-stone-900`,
};

export const styleToBgGradientActiveMap = {
  primary: `active:from-primary-brightDark active:to-primary-light ${gradiantClassesBase}`,
  info: `active:from-info-brightDark active:to-info-light ${gradiantClassesBase}`,
  success: `active:from-success-brightDark active:to-success-light ${gradiantClassesBase}`,
  warning: `active:from-warning-brightDark active:to-warning-light ${gradiantClassesBase}`,
  danger: `active:from-danger-brightDark active:to-danger-light ${gradiantClassesBase}`,
  alert: `active:from-alert-brightDark active:to-alert-light ${gradiantClassesBase}`,
  tip: `active:from-tip-brightDark active:to-tip-light ${gradiantClassesBase}`,
  muted: `active:from-stone-300 active:to-stone-300 ${gradiantClassesBase} dark:active:from-stone-900 dark:active:to-stone-800`,
  strong: `active:from-stone-950 active:to-stone-900 ${gradiantClassesBase} dark:active:from-stone-100 dark:active:to-stone-200`,
  default: `active:from-stone-200/50 active:to-stone-100/50 dark:active:from-stone-950 dark:active:to-black ${gradiantClassesBase}`,
};

export const shadowClassesBase = "shadow-sm";

export const styleToHoverShadowMap = {
  primary: `${shadowClassesBase} shadow-blue/20 hover:shadow-blue/40`,
  info: `${shadowClassesBase} shadow-purple/20 hover:shadow-purple/30`,
  success: `${shadowClassesBase} shadow-green/20 hover:shadow-green/30`,
  warning: `${shadowClassesBase} shadow-orange/20 hover:shadow-orange/30`,
  danger: `${shadowClassesBase} shadow-red/20 hover:shadow-red/30`,
  alert: `${shadowClassesBase} shadow-yellow/20 hover:shadow-yellow/30`,
  tip: `${shadowClassesBase} shadow-cyan/20 hover:shadow-cyan/30`,
  muted: `${shadowClassesBase} shadow-stone-200/20 hover:shadow-stone-200/30 dark:shadow-stone-600/20 dark:hover:shadow-stone-600/30`,
  strong: `${shadowClassesBase} shadow-stone-900/20 hover:shadow-stone-900/30 dark:shadow-white/20 dark:hover:shadow-white/30`,
  default: `${shadowClassesBase} shadow-stone-600/20 hover:shadow-stone-600/30 dark:shadow-stone-200/20 dark:hover:shadow-stone-200/30`,
};

// Enhanced glass effect utilities - cleaner and more transparent
export const glassBaseClasses =
  "relative overflow-hidden backdrop-blur-xs shadow-[0_6px_6px_rgba(0,0,0,0.2),0_0_20px_rgba(0,0,0,0.1)] transition-all duration-400 [transition-timing-function:cubic-bezier(0.175,0.885,0.32,2.2)]";

export const glassOverlayClasses =
  "before:content-[''] before:absolute before:inset-0 before:rounded-[inherit] before:pointer-events-none before:z-[1]";

export const glassSpecularClasses =
  "after:content-[''] after:absolute after:inset-0 after:rounded-[inherit] after:pointer-events-none after:z-[2]";

export const glassContentClasses = "relative z-[3]";

// Clean glass effects with transparent intent colors and proper specular highlights
export const styleToGlassMap = {
  primary: `${glassBaseClasses} ${glassOverlayClasses} ${glassSpecularClasses} before:bg-blue-500/20 after:shadow-[inset_1px_1px_0_rgba(59,130,246,0.75),inset_0_0_5px_rgba(59,130,246,0.4)]`,
  info: `${glassBaseClasses} ${glassOverlayClasses} ${glassSpecularClasses} before:bg-purple-500/20 after:shadow-[inset_1px_1px_0_rgba(147,51,234,0.75),inset_0_0_5px_rgba(147,51,234,0.4)]`,
  success: `${glassBaseClasses} ${glassOverlayClasses} ${glassSpecularClasses} before:bg-green-500/20 after:shadow-[inset_1px_1px_0_rgba(34,197,94,0.75),inset_0_0_5px_rgba(34,197,94,0.4)]`,
  warning: `${glassBaseClasses} ${glassOverlayClasses} ${glassSpecularClasses} before:bg-orange-500/20 after:shadow-[inset_1px_1px_0_rgba(249,115,22,0.75),inset_0_0_5px_rgba(249,115,22,0.4)]`,
  danger: `${glassBaseClasses} ${glassOverlayClasses} ${glassSpecularClasses} before:bg-red-500/20 after:shadow-[inset_1px_1px_0_rgba(239,68,68,0.75),inset_0_0_5px_rgba(239,68,68,0.4)]`,
  alert: `${glassBaseClasses} ${glassOverlayClasses} ${glassSpecularClasses} before:bg-yellow-500/20 after:shadow-[inset_1px_1px_0_rgba(234,179,8,0.75),inset_0_0_5px_rgba(234,179,8,0.4)]`,
  tip: `${glassBaseClasses} ${glassOverlayClasses} ${glassSpecularClasses} before:bg-cyan-500/20 after:shadow-[inset_1px_1px_0_rgba(6,182,212,0.75),inset_0_0_5px_rgba(6,182,212,0.4)]`,
  muted: `${glassBaseClasses} ${glassOverlayClasses} ${glassSpecularClasses} before:bg-stone-500/20 after:shadow-[inset_1px_1px_0_rgba(120,113,108,0.75),inset_0_0_5px_rgba(120,113,108,0.4)]`,
  strong: `${glassBaseClasses} ${glassOverlayClasses} ${glassSpecularClasses} before:bg-stone-800/20 after:shadow-[inset_1px_1px_0_rgba(68,64,60,0.75),inset_0_0_5px_rgba(68,64,60,0.4)]`,
  default: `${glassBaseClasses} ${glassOverlayClasses} ${glassSpecularClasses} before:bg-white/15 after:shadow-[inset_1px_1px_0_rgba(255,255,255,0.75),inset_0_0_5px_rgba(255,255,255,0.4)]`,
};

// Subtle glass variants with cleaner transparency
export const styleToGlassSubtleMap = {
  primary: `${glassBaseClasses} ${glassOverlayClasses} before:bg-blue-500/10 after:shadow-[inset_1px_1px_0_rgba(59,130,246,0.4),inset_0_0_5px_rgba(59,130,246,0.2)]`,
  info: `${glassBaseClasses} ${glassOverlayClasses} before:bg-purple-500/10 after:shadow-[inset_1px_1px_0_rgba(147,51,234,0.4),inset_0_0_5px_rgba(147,51,234,0.2)]`,
  success: `${glassBaseClasses} ${glassOverlayClasses} before:bg-green-500/10 after:shadow-[inset_1px_1px_0_rgba(34,197,94,0.4),inset_0_0_5px_rgba(34,197,94,0.2)]`,
  warning: `${glassBaseClasses} ${glassOverlayClasses} before:bg-orange-500/10 after:shadow-[inset_1px_1px_0_rgba(249,115,22,0.4),inset_0_0_5px_rgba(249,115,22,0.2)]`,
  danger: `${glassBaseClasses} ${glassOverlayClasses} before:bg-red-500/10 after:shadow-[inset_1px_1px_0_rgba(239,68,68,0.4),inset_0_0_5px_rgba(239,68,68,0.2)]`,
  alert: `${glassBaseClasses} ${glassOverlayClasses} before:bg-yellow-500/10 after:shadow-[inset_1px_1px_0_rgba(234,179,8,0.4),inset_0_0_5px_rgba(234,179,8,0.2)]`,
  tip: `${glassBaseClasses} ${glassOverlayClasses} before:bg-cyan-500/10 after:shadow-[inset_1px_1px_0_rgba(6,182,212,0.4),inset_0_0_5px_rgba(6,182,212,0.2)]`,
  muted: `${glassBaseClasses} ${glassOverlayClasses} before:bg-stone-500/10 after:shadow-[inset_1px_1px_0_rgba(120,113,108,0.4),inset_0_0_5px_rgba(120,113,108,0.2)]`,
  strong: `${glassBaseClasses} ${glassOverlayClasses} before:bg-stone-800/10 after:shadow-[inset_1px_1px_0_rgba(68,64,60,0.4),inset_0_0_5px_rgba(68,64,60,0.2)]`,
  default: `${glassBaseClasses} ${glassOverlayClasses} before:bg-white/8 after:shadow-[inset_1px_1px_0_rgba(255,255,255,0.4),inset_0_0_5px_rgba(255,255,255,0.2)]`,
};

// Clean glass button classes with shared properties
export const glassButtonBaseClasses =
  "relative backdrop-blur-sm overflow-hidden transition-all duration-400 ease-in-out border-0 bg-gradient-to-br shadow-[0_6px_6px_rgba(0,0,0,0.2),0_0_20px_rgba(0,0,0,0.1)]";

export const styleToGlassButtonClassMap = {
  primary: `${glassButtonBaseClasses} from-primary-light to-primary-dark hover:from-primary-dark hover:to-primary-brightLight active:from-primary-brightDark active:to-primary-light shadow-[inset_0_0_0_0.4px_rgba(59,130,246,0.2),inset_0_0.4px_0_rgba(147,197,253,0.4),inset_0_-0.4px_0_rgba(30,64,175,0.3)]`,
  info: `${glassButtonBaseClasses} from-info-light to-info-dark hover:from-info-dark hover:to-info-brightLight active:from-info-brightDark active:to-info-light shadow-[inset_0_0_0_0.4px_rgba(147,51,234,0.2),inset_0_0.4px_0_rgba(221,214,254,0.4),inset_0_-0.4px_0_rgba(88,28,135,0.3)]`,
  success: `${glassButtonBaseClasses} from-success-light to-success-dark hover:from-success-brightDark hover:to-success-brightLight active:from-success-dark hover:to-success-light shadow-[inset_0_0_0_0.4px_rgba(34,197,94,0.2),inset_0_0.4px_0_rgba(187,247,208,0.4),inset_0_-0.4px_0_rgba(22,101,52,0.3)]`,
  warning: `${glassButtonBaseClasses} from-warning-light to-warning-dark hover:from-warning-dark hover:to-warning-brightLight active:from-warning-brightDark active:to-warning-light shadow-[inset_0_0_0_0.4px_rgba(249,115,22,0.2),inset_0_0.4px_0_rgba(254,240,138,0.4),inset_0_-0.4px_0_rgba(154,52,18,0.3)]`,
  danger: `${glassButtonBaseClasses} from-danger-light to-danger-dark hover:from-danger-brightDark hover:to-danger-brightLight active:from-danger-dark active:to-danger-light shadow-[inset_0_0_0_0.4px_rgba(239,68,68,0.2),inset_0_0.4px_0_rgba(254,202,202,0.4),inset_0_-0.4px_0_rgba(153,27,27,0.3)]`,
  alert: `${glassButtonBaseClasses} from-alert-light to-alert-dark hover:from-alert-dark hover:to-alert-brightLight active:from-alert-brightDark active:to-alert-light shadow-[inset_0_0_0_0.4px_rgba(234,179,8,0.2),inset_0_0.4px_0_rgba(254,249,195,0.4),inset_0_-0.4px_0_rgba(133,77,14,0.3)]`,
  tip: `${glassButtonBaseClasses} from-tip-light to-tip-dark hover:from-tip-dark hover:to-tip-brightLight active:from-tip-brightDark active:to-tip-light shadow-[inset_0_0_0_0.4px_rgba(6,182,212,0.2),inset_0_0.4px_0_rgba(207,250,254,0.4),inset_0_-0.4px_0_rgba(21,94,117,0.3)]`,
  muted: `${glassButtonBaseClasses} from-stone-300 to-stone-600 hover:from-stone-600 hover:to-stone-200 active:from-stone-700 active:to-stone-300 shadow-[inset_0_0_0_0.4px_rgba(120,113,108,0.2),inset_0_0.4px_0_rgba(245,245,244,0.4),inset_0_-0.4px_0_rgba(68,64,60,0.3)]`,
  strong: `${glassButtonBaseClasses} from-stone-700 to-stone-900 hover:from-stone-900 hover:to-stone-600 active:from-stone-800 active:to-stone-700 shadow-[inset_0_0_0_0.4px_rgba(68,64,60,0.2),inset_0_0.4px_0_rgba(168,162,158,0.4),inset_0_-0.4px_0_rgba(41,37,36,0.3)]`,
  default: `${glassButtonBaseClasses} from-stone-200 to-stone-400 hover:from-stone-400 hover:to-stone-100 active:from-stone-500 active:to-stone-200 shadow-[inset_0_0_0_0.4px_rgba(255,255,255,0.2),inset_0_0.4px_0_rgba(255,255,255,0.4),inset_0_-0.4px_0_rgba(156,163,175,0.3)]`,
};

const focusRingClassesBase =
  "focus:outline-none focus-visible:ring focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-opacity-10";

export const styleToButtonStateMap = {
  primary: `${styleToBgGradientActiveMap.primary} ${styleToBgGradientHoverMap.primary} ${focusRingClassesBase} focus:ring-primary`,
  info: `${styleToBgGradientActiveMap.info} ${styleToBgGradientHoverMap.info} ${focusRingClassesBase} focus:ring-info`,
  success: `${styleToBgGradientActiveMap.success} ${styleToBgGradientHoverMap.success} ${focusRingClassesBase} focus:ring-success`,
  warning: `${styleToBgGradientActiveMap.warning} ${styleToBgGradientHoverMap.warning} ${focusRingClassesBase} focus:ring-warning`,
  danger: `${styleToBgGradientActiveMap.danger} ${styleToBgGradientHoverMap.danger} ${focusRingClassesBase} focus:ring-danger`,
  alert: `${styleToBgGradientActiveMap.alert} ${styleToBgGradientHoverMap.alert} ${focusRingClassesBase} focus:ring-alert`,
  tip: `${styleToBgGradientActiveMap.tip} ${styleToBgGradientHoverMap.tip} ${focusRingClassesBase} focus:ring-tip`,
  muted: `${styleToBgGradientActiveMap.muted} ${styleToBgGradientHoverMap.muted} ${focusRingClassesBase} focus:ring-stone-200 dark:focus:ring-stone-900`,
  strong: `${styleToBgGradientActiveMap.strong} ${styleToBgGradientHoverMap.strong} ${focusRingClassesBase} focus:ring-stone-800 dark:focus:ring-stone-200`,
  default: `${styleToBgGradientActiveMap.default} ${styleToBgGradientHoverMap.default} ${focusRingClassesBase} focus:ring-black dark:focus:ring-white`,
};

export const variantStyleToButtonStateMap = {
  outline: `${focusRingClassesBase}`,
  inverted: `${focusRingClassesBase}`,
  ghost: `${focusRingClassesBase}`,
  text: `${focusRingClassesBase}`,
};
