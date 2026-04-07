// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Pre-rendered HTML pages for the vgi-rpc HTTP server.
 * Matches the styling of the Python and Go implementations.
 */

import type { MethodDefinition } from "../types.js";

export const LOGO_URL = "https://vgi-rpc-python.query.farm/assets/logo-hero.png";

export const FONTS = `<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">`;

export const ERROR_PAGE_STYLE = `<style>
body { font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 600px;
       margin: 0 auto; padding: 60px 20px 0; color: #2c2c1e; text-align: center;
       background: #faf8f0; }
.logo { margin-bottom: 24px; }
.logo img { width: 120px; height: 120px; border-radius: 50%;
             box-shadow: 0 4px 24px rgba(0,0,0,0.12); }
h1 { color: #2d5016; margin-bottom: 8px; font-weight: 700; }
code { font-family: 'JetBrains Mono', monospace; background: #f0ece0;
        padding: 2px 6px; border-radius: 3px; font-size: 0.9em; color: #2c2c1e; }
a { color: #2d5016; text-decoration: none; }
a:hover { color: #4a7c23; }
p { line-height: 1.7; color: #6b6b5a; }
.detail { margin-top: 12px; padding: 12px 16px; background: #f0ece0;
           border-radius: 6px; font-size: 0.9em; color: #6b6b5a; }
footer { margin-top: 48px; padding: 20px 0; border-top: 1px solid #f0ece0;
          color: #6b6b5a; font-size: 0.85em; line-height: 1.8; }
footer a { color: #2d5016; font-weight: 600; }
footer a:hover { color: #4a7c23; }
</style>`;

function escapeHtml(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function arrowTypeToString(type: import("@query-farm/apache-arrow").DataType): string {
  const id = type.typeId;
  // Match the human-friendly type names used by the Python reference implementation
  if (id === 5) return "str"; // Utf8
  if (id === 4) return "bytes"; // Binary
  if (id === 2) return "int"; // Int32/Int64
  if (id === 3) return "float"; // Float32/Float64
  if (id === 6) return "bool"; // Bool
  if (id === 12) return "list"; // List
  if (id === 17) return "map"; // Map
  if (id === 24) return "enum"; // Dictionary
  return type.toString();
}

// ---------------------------------------------------------------------------
// Landing page
// ---------------------------------------------------------------------------

export function buildLandingPage(
  protocolName: string,
  serverId: string,
  describePath: string | null,
  repoUrl: string | null,
): string {
  const links: string[] = [];
  if (describePath) {
    links.push(`<a class="primary" href="${escapeHtml(describePath)}">View service API</a>`);
  }
  if (repoUrl) {
    links.push(`<a href="${escapeHtml(repoUrl)}">Source repository</a>`);
  }
  links.push(`<a href="https://vgi-rpc.query.farm">Learn more about <code>vgi-rpc</code></a>`);

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHtml(protocolName)} \u2014 vgi-rpc</title>
${FONTS}
<style>
body { font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 600px;
       margin: 0 auto; padding: 60px 20px 0; color: #2c2c1e; text-align: center;
       background: #faf8f0; }
.logo { margin-bottom: 24px; }
.logo img { width: 140px; height: 140px; border-radius: 50%;
             box-shadow: 0 4px 24px rgba(0,0,0,0.12); }
h1 { color: #2d5016; margin-bottom: 8px; font-weight: 700; }
code { font-family: 'JetBrains Mono', monospace; background: #f0ece0;
        padding: 2px 6px; border-radius: 3px; font-size: 0.9em; color: #2c2c1e; }
a { color: #2d5016; text-decoration: none; }
a:hover { color: #4a7c23; }
p { line-height: 1.7; color: #6b6b5a; }
.meta { font-size: 0.9em; color: #6b6b5a; }
.links { margin-top: 28px; display: flex; flex-wrap: wrap; justify-content: center; gap: 8px; }
.links a { display: inline-block; padding: 8px 18px; border-radius: 6px;
            border: 1px solid #4a7c23; color: #2d5016; font-weight: 600;
            font-size: 0.9em; transition: all 0.2s ease; }
.links a:hover { background: #4a7c23; color: #fff; }
.links a.primary { background: #2d5016; color: #fff; border-color: #2d5016; }
.links a.primary:hover { background: #4a7c23; border-color: #4a7c23; }
footer { margin-top: 48px; padding: 20px 0; border-top: 1px solid #f0ece0;
          color: #6b6b5a; font-size: 0.85em; }
footer a { color: #2d5016; font-weight: 600; }
footer a:hover { color: #4a7c23; }
</style>
</head>
<body>
<div class="logo">
  <img src="${LOGO_URL}" alt="vgi-rpc logo">
</div>
<h1>${escapeHtml(protocolName)}</h1>
<p class="meta">Powered by <code>vgi-rpc</code> (TypeScript) &middot; server <code>${escapeHtml(serverId)}</code></p>
<p>This is a <code>vgi-rpc</code> service endpoint.</p>
<div class="links">
${links.join("\n")}
</div>
<footer>
  &copy; 2026 &#x1F69C; <a href="https://query.farm">Query.Farm LLC</a>
</footer>
</body>
</html>`;
}

// ---------------------------------------------------------------------------
// 404 page
// ---------------------------------------------------------------------------

export function buildNotFoundPage(prefix: string, protocolName: string): string {
  const nameFragment = protocolName ? ` (<strong>${escapeHtml(protocolName)}</strong>)` : "";
  const prefixDisplay = prefix || "/";
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>404 \u2014 vgi-rpc</title>
${FONTS}
${ERROR_PAGE_STYLE}
</head>
<body>
<div class="logo">
  <img src="${LOGO_URL}" alt="vgi-rpc logo">
</div>
<h1>404 \u2014 Not Found</h1>
<p>This is a <code>vgi-rpc</code> service endpoint${nameFragment}.</p>
<p>RPC methods are available under <code>${escapeHtml(prefixDisplay)}/&lt;method&gt;</code>.</p>
<footer>
  Powered by <a href="https://vgi-rpc.query.farm"><code>vgi-rpc</code></a>
</footer>
</body>
</html>`;
}

// ---------------------------------------------------------------------------
// Describe / API reference page
// ---------------------------------------------------------------------------

function buildMethodCard(method: MethodDefinition): string {
  const name = escapeHtml(method.name);
  const isUnary = method.type === "unary";
  const hasHeader = !!method.headerSchema;

  // Badges — match Python reference (unary/stream/header only)
  const badges: string[] = [];
  badges.push(
    isUnary ? `<span class="badge badge-unary">unary</span>` : `<span class="badge badge-stream">stream</span>`,
  );
  if (hasHeader) badges.push(`<span class="badge badge-header">header</span>`);

  // Parameters table
  let paramsHtml = "";
  const paramsSchema = method.paramsSchema;
  if (paramsSchema.fields.length > 0) {
    const rows = paramsSchema.fields.map((f) => {
      const paramName = escapeHtml(f.name);
      const paramType = escapeHtml(arrowTypeToString(f.type));
      const defaultVal =
        method.defaults && f.name in method.defaults ? escapeHtml(JSON.stringify(method.defaults[f.name])) : "&mdash;";
      return `<tr><td><code>${paramName}</code></td><td><code>${paramType}</code></td><td>${defaultVal}</td><td>&mdash;</td></tr>`;
    });
    paramsHtml = `<div class="section-label">Parameters</div>
<table><tr><th>Name</th><th>Type</th><th>Default</th><th>Description</th></tr>
${rows.join("\n")}
</table>`;
  } else {
    paramsHtml = `<p class="no-params">No parameters</p>`;
  }

  // Returns table (unary only)
  let returnsHtml = "";
  if (isUnary && method.resultSchema.fields.length > 0) {
    const rows = method.resultSchema.fields.map((f) => {
      return `<tr><td><code>${escapeHtml(f.name)}</code></td><td><code>${escapeHtml(arrowTypeToString(f.type))}</code></td></tr>`;
    });
    returnsHtml = `<div class="section-label">Returns</div>
<table><tr><th>Name</th><th>Type</th></tr>
${rows.join("\n")}
</table>`;
  }

  // Header table (streams with headers)
  let headerHtml = "";
  if (hasHeader && method.headerSchema && method.headerSchema.fields.length > 0) {
    const rows = method.headerSchema.fields.map((f) => {
      return `<tr><td><code>${escapeHtml(f.name)}</code></td><td><code>${escapeHtml(arrowTypeToString(f.type))}</code></td></tr>`;
    });
    headerHtml = `<div class="section-label">Stream Header</div>
<table><tr><th>Name</th><th>Type</th></tr>
${rows.join("\n")}
</table>`;
  }

  // Docstring
  const docHtml = method.doc ? `<p class="docstring">${escapeHtml(method.doc)}</p>` : "";

  return `<div class="card">
<div class="card-header">
<span class="method-name">${name}</span>
${badges.join("\n")}
</div>
${docHtml}
${paramsHtml}
${returnsHtml}
${headerHtml}
</div>`;
}

export function buildDescribePage(
  protocolName: string,
  serverId: string,
  methods: Map<string, MethodDefinition>,
  repoUrl: string | null,
): string {
  const sortedMethods = [...methods.entries()]
    .filter(([name]) => name !== "__describe__")
    .sort(([a], [b]) => a.localeCompare(b));

  const cards = sortedMethods.map(([, method]) => buildMethodCard(method)).join("\n");

  const repoLink = repoUrl ? ` &middot; <a href="${escapeHtml(repoUrl)}">Source</a>` : "";

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHtml(protocolName)} API Reference \u2014 vgi-rpc</title>
${FONTS}
<style>
body { font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 900px;
       margin: 0 auto; padding: 40px 20px 0; color: #2c2c1e; background: #faf8f0; }
.header { text-align: center; margin-bottom: 40px; }
.header .logo img { width: 80px; height: 80px; border-radius: 50%;
                     box-shadow: 0 3px 16px rgba(0,0,0,0.10); }
.header h1 { margin-bottom: 4px; color: #2d5016; font-weight: 700; }
.header .subtitle { color: #6b6b5a; font-size: 1.1em; margin-top: 0; }
.header .meta { color: #6b6b5a; font-size: 0.9em; }
.header .meta a { color: #2d5016; font-weight: 600; }
.header .meta a:hover { color: #4a7c23; }
code { font-family: 'JetBrains Mono', monospace; background: #f0ece0;
        padding: 2px 6px; border-radius: 3px; font-size: 0.85em; color: #2c2c1e; }
a { color: #2d5016; text-decoration: none; }
a:hover { color: #4a7c23; }
.card { border: 1px solid #f0ece0; border-radius: 8px; padding: 20px;
         margin-bottom: 16px; background: #fff; }
.card:hover { border-color: #c8a43a; }
.card-header { display: flex; align-items: center; gap: 10px; margin-bottom: 12px; }
.method-name { font-family: 'JetBrains Mono', monospace; font-size: 1.1em; font-weight: 600;
                color: #2d5016; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
          font-size: 0.75em; font-weight: 600; text-transform: uppercase;
          letter-spacing: 0.03em; }
.badge-unary { background: #e8f5e0; color: #2d5016; }
.badge-stream { background: #e0ecf5; color: #1a4a6b; }
.badge-exchange { background: #f5e6f0; color: #6b234a; }
.badge-producer { background: #e0f0f5; color: #1a5a6b; }
.badge-header { background: #f5eee0; color: #6b4423; }
.docstring { color: #6b6b5a; font-size: 0.9em; margin-top: 0; }
table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
th { text-align: left; padding: 8px 10px; background: #f0ece0; color: #2c2c1e;
      font-weight: 600; border-bottom: 2px solid #e0dcd0; }
td { padding: 8px 10px; border-bottom: 1px solid #f0ece0; }
td code { font-size: 0.85em; }
.no-params { color: #6b6b5a; font-style: italic; font-size: 0.9em; }
.section-label { font-size: 0.8em; font-weight: 600; text-transform: uppercase;
                  letter-spacing: 0.05em; color: #6b6b5a; margin-top: 14px;
                  margin-bottom: 6px; }
footer { text-align: center; margin-top: 48px; padding: 20px 0;
          border-top: 1px solid #f0ece0; color: #6b6b5a; font-size: 0.85em; }
footer a { color: #2d5016; font-weight: 600; }
footer a:hover { color: #4a7c23; }
</style>
</head>
<body>
<div class="header">
  <div class="logo">
    <img src="${LOGO_URL}" alt="vgi-rpc logo">
  </div>
  <h1>${escapeHtml(protocolName)}</h1>
  <p class="subtitle">API Reference</p>
  <p class="meta">Powered by <code>vgi-rpc</code> (TypeScript) &middot; server <code>${escapeHtml(serverId)}</code>${repoLink}</p>
</div>
${cards}
<footer>
  <a href="https://vgi-rpc.query.farm">Learn more about <code>vgi-rpc</code></a>
  &middot;
  &copy; 2026 &#x1F69C; <a href="https://query.farm">Query.Farm LLC</a>
</footer>
</body>
</html>`;
}
