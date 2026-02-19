import fs from "fs";
import path from "path";

const DOCS_ROOT = path.resolve("public/docs");
const AGENTS_MD = path.resolve("../../AGENTS.md");

const START = "<!--DOCS_INDEX_START-->";
const END = "<!--DOCS_INDEX_END-->";

const FRAMEWORKS = [
  "react",
  "react-native",
  "react-native-expo",
  "svelte",
  "vanilla",
];

const excludes = ['coming-soon.md'];

function collectMarkdownFiles(dir) {
  let files = [];
  if (!fs.existsSync(dir)) return [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...collectMarkdownFiles(fullPath));
    } else if (entry.isFile() && entry.name.endsWith(".md") && !excludes.includes(entry.name)) {
      files.push(fullPath);
    }
  }
  return files;
}

function parseMarkdownStructure(content) {
  const lines = content.split("\n");
  let title = null;
  const sections = [];

  for (const line of lines) {
    const tMatch = line.match(/^# (.+)$/);
    if (tMatch && !title) {
      title = tMatch[1].trim();
      continue;
    }
    const sMatch = line.match(/^## (.+)$/);
    if (sMatch) {
      sections.push(sMatch[1].trim());
    }
  }
  return { title, sections };
}

function buildGroupedIndex(files) {
  const dirMap = new Map();

  for (const file of files) {
    const relativePath = path.relative(DOCS_ROOT, file).replace(/\\/g, "/");
    const content = fs.readFileSync(file, "utf8");
    const { sections } = parseMarkdownStructure(content);

    let framework = "b"; // Default internal key for base
    let canonicalPath = relativePath;

    for (const fw of FRAMEWORKS) {
      if (relativePath.startsWith(`${fw}/`)) {
        framework = fw;
        canonicalPath = relativePath.slice(fw.length + 1);
        break;
      }
    }

    const dir = path.dirname(canonicalPath);
    const fileName = path.basename(canonicalPath, ".md"); // Strip .md to save space

    if (!dirMap.has(dir)) dirMap.set(dir, new Map());
    const filesInDir = dirMap.get(dir);

    if (!filesInDir.has(fileName)) {
      filesInDir.set(fileName, { variants: new Set(), sections: new Set() });
    }

    const data = filesInDir.get(fileName);
    data.variants.add(framework);
    // Grab only first 3 headings to keep it ultra-lean
    sections.slice(0, 3).forEach(s => data.sections.add(s));
  }

  const lines = [];
  lines.push("[Jazz Docs Index]|root:homepage/homepage/public/docs");
  lines.push("|LEGEND: b=base (no prefix), r=react, rn=react-native, rne=react-native-expo, s=svelte, v=vanilla, ss=server-side");
  lines.push("|RULES: All files are .md. Resolve `[fw]/[dir]/[file].md`. If variant is 'b', path is `[dir]/[file].md`.");
  lines.push("");

  const sortedDirs = Array.from(dirMap.keys()).sort();

  for (const dir of sortedDirs) {
    const filesInDir = dirMap.get(dir);
    const fileEntries = [];

    // Map long framework names to short codes for the index
    const fwMap = {
      "base": "b", "react": "r", "react-native": "rn",
      "react-native-expo": "rne", "svelte": "s", "vanilla": "v", "server-side": "ss"
    };

    for (const [fileName, data] of filesInDir) {
      const vStr = Array.from(data.variants)
        .map(v => fwMap[v] || v) // Use short code if exists
        .sort()
        .join("|");

      const sectionStr = data.sections.size > 0
        ? `{#${Array.from(data.sections).join('#')}}`
        : "";

      fileEntries.push(`${fileName}:${vStr}${sectionStr}`);
    }

    lines.push(`|${dir}:{${fileEntries.join(',')}}`);
  }

  return lines.join("\n");
}

function replaceBetweenSentinels(file, replacement) {
  if (!fs.existsSync(file)) {
    throw new Error(`Target file not found: ${file}`);
  }
  const text = fs.readFileSync(file, "utf8");
  const startIdx = text.indexOf(START);
  const endIdx = text.indexOf(END);

  if (startIdx === -1 || endIdx === -1) {
    throw new Error("Sentinels not found. Ensure and exist in AGENTS.md");
  }

  const before = text.substring(0, startIdx + START.length);
  const after = text.substring(endIdx);

  return `${before}\n\n${replacement}\n\n${after}`;
}

// ---- Run ----
try {
  const mdFiles = collectMarkdownFiles(DOCS_ROOT);
  const indexContent = buildGroupedIndex(mdFiles);
  const updatedFileContent = replaceBetweenSentinels(AGENTS_MD, indexContent);

  fs.writeFileSync(AGENTS_MD, updatedFileContent, "utf8");
  console.log(`✅ Success: Indexed ${mdFiles.length} files into ${path.basename(AGENTS_MD)}`);
} catch (err) {
  console.error(`❌ Error: ${err.message}`);
}