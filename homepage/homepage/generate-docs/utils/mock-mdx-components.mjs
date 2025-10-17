import { createElement, Children } from "react";

// CURRENT_FRAMEWORK will be defined at build-time via esbuild's define option
// It will be replaced with the actual framework string or null

export const TabbedCodeGroup = ({ children }) => {
  const currentFramework = CURRENT_FRAMEWORK;
  
  if (!currentFramework) {
    // No framework set, render all tabs with labels
    return createElement('div', null, children);
  }

  // Filter children to find matching framework tab
  const childArray = Children.toArray(children);
  const matchingChild = childArray.find(child => {
    return child.props?.value === currentFramework;
  });

  // Render matching tab without the label wrapper, or first tab as fallback
  const tabToRender = matchingChild || childArray[0];
  if (tabToRender) {
    // Just render the content directly, not the wrapper
    return createElement('div', null, tabToRender.props.children);
  }

  return null;
};

export const TabbedCodeGroupItem = ({ children, label, value }) => {
  const currentFramework = CURRENT_FRAMEWORK;
  
  if (!currentFramework) {
    // No framework set, render with label
    return createElement('div', null, 
      createElement('h5', null, `${label}:`),
      children
    );
  }

  // When framework is set, only render if this tab matches
  if (value === currentFramework) {
    return children;
  }

  return null;
};

export const ContentByFramework = ({ children, framework }) => {
  const currentFramework = CURRENT_FRAMEWORK;
  
  if (!currentFramework) {
    // No framework set, render with markers
    return createElement('div', null, 
      createElement('p', {}, `--- Section applies only to ${framework} ---`), 
      children, 
      createElement('p', null, `--- End of ${framework} specific section ---`)
    );
  }

  // Check if framework prop matches current framework
  if (framework === currentFramework) {
    return children;
  }

  // Handle array of frameworks
  if (Array.isArray(framework) && framework.includes(currentFramework)) {
    return children;
  }

  return null;
};

export const CodeGroup = ({ children }) => {
  return createElement('div', null, children);
};

export const FileName = ({ children }) => {
  return createElement('div', null, createElement('strong', {}, 'File name: ', children));
};

export const Alert = ({ children, title, variant }) => {
  return createElement('div', {},  createElement('strong', null, `${variant && variant[0].toUpperCase() + variant.slice(1) + ": "}${title || ''} `), children);
};

export const TextLink = ({ children, href }) => {
  return createElement('a', { href }, children);
};

export const FileDownloadLink = ({ children, href }) => {
  return createElement('a', { href, download: true }, children);
};

// Just don't render these at all
const emptyElement = () => null;
export { emptyElement as ReactLogo, emptyElement as SvelteLogo, emptyElement as JazzLogo, emptyElement as JazzIcon, emptyElement as VanillaLogo };