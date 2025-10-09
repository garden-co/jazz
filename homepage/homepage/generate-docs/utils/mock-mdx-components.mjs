import { createElement } from "react";

export const TabbedCodeGroup = ({ children }) => {
  return createElement('div', null, children);
};

export const TabbedCodeGroupItem = ({ children, label }) => {
  return createElement('div', null, 
    createElement('h5', null, `${label}:`), // H5 seems OK? We need to mark this out as some sort of section, so I think this is the best way.
    children
  );
};

export const ContentByFramework = ({ children, framework }) => {
  return createElement('div', null, createElement('p', {}, `--- Section applies only to ${framework} ---`), children, createElement('p', null, `--- End of ${framework} specific section ---`));
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
export { emptyElement as ReactLogo, emptyElement as SvelteLogo, emptyElement as JazzLogo, emptyElement as JazzIcon };