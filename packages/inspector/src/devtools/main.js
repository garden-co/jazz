const panelTitle = "Jazz Inspector";
const panelIconPath = "";
const panelPagePath = "devtools-tab.html";

chrome.devtools.panels.create(panelTitle, panelIconPath, panelPagePath);
