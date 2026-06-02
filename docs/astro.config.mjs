import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// query.farm "farm-theme" for code blocks — dark-green editor surface with
// green/yellow tokens, matching query-farm-astro's Shiki theme.
const farmTheme = {
  name: "farm-theme",
  type: "dark",
  colors: {
    "editor.background": "#0d2818",
    "editor.foreground": "#e8f5e9",
  },
  settings: [
    { scope: ["comment", "punctuation.definition.comment"], settings: { foreground: "#a5d6a7", fontStyle: "italic" } },
    { scope: ["string", "string.quoted"], settings: { foreground: "#c5e1a5" } },
    { scope: ["keyword", "storage.type", "storage.modifier"], settings: { foreground: "#66bb6a", fontStyle: "bold" } },
    { scope: ["entity.name.function", "support.function"], settings: { foreground: "#ffeb3b" } },
    { scope: ["constant.numeric", "constant.language"], settings: { foreground: "#ffab91" } },
    { scope: ["variable", "entity.name"], settings: { foreground: "#e8f5e9" } },
    { scope: ["punctuation"], settings: { foreground: "#c8e6c9" } },
    { scope: ["constant.other", "support.type"], settings: { foreground: "#fff59d" } },
    { scope: ["keyword.operator"], settings: { foreground: "#e8f5e9" } },
  ],
};

export default defineConfig({
  site: "https://vgi-rpc-typescript.query.farm",
  integrations: [
    starlight({
      title: "vgi-rpc",
      description:
        "TypeScript RPC server library powered by Apache Arrow IPC.",
      expressiveCode: {
        themes: [farmTheme],
        styleOverrides: {
          borderRadius: "0.5rem",
          borderColor: "#2d5016",
          codeFontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace",
        },
      },
      logo: {
        src: "./public/logo-hero.png",
        alt: "VGI-RPC Logo",
      },
      favicon: "/logo-hero.png",
      head: [
        {
          tag: "meta",
          attrs: {
            property: "og:image",
            content:
              "https://vgi-rpc-typescript.query.farm/og-image.png",
          },
        },
        {
          tag: "meta",
          attrs: {
            property: "og:image:width",
            content: "1200",
          },
        },
        {
          tag: "meta",
          attrs: {
            property: "og:image:height",
            content: "630",
          },
        },
        {
          tag: "meta",
          attrs: {
            property: "og:image:alt",
            content:
              "vgi-rpc: TypeScript RPC powered by Apache Arrow",
          },
        },
      ],
      customCss: ["./src/styles/custom.css"],
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/Query-farm/vgi-rpc-typescript",
        },
      ],
      credits: false,
      lastUpdated: true,
      components: {
        Footer: "./src/components/Footer.astro",
      },
      sidebar: [
        {
          label: "vgi-rpc Project",
          link: "https://vgi-rpc.query.farm",
          attrs: { target: "_blank", rel: "noopener" },
        },
        {
          label: "Getting Started",
          items: [
            { label: "Installation", slug: "getting-started/installation" },
            {
              label: "Your First Server",
              slug: "getting-started/your-first-server",
            },
          ],
        },
        {
          label: "Server",
          items: [
            { label: "Unary Methods", slug: "guides/unary-methods" },
            { label: "Producer Streams", slug: "guides/producer-streams" },
            { label: "Exchange Streams", slug: "guides/exchange-streams" },
            { label: "Stream Headers", slug: "guides/stream-headers" },
            { label: "Schema Shorthand", slug: "guides/schema-shorthand" },
            { label: "Output Collector", slug: "guides/output-collector" },
            { label: "Client Logging", slug: "guides/client-logging" },
            { label: "Error Handling", slug: "guides/error-handling" },
            { label: "HTTP Transport", slug: "guides/http-transport" },
            { label: "Compression", slug: "guides/compression" },
            { label: "Large Payloads", slug: "guides/large-payloads" },
            { label: "Sticky Sessions", slug: "guides/sticky-sessions" },
            { label: "Authentication", slug: "guides/authentication" },
            { label: "OAuth & PKCE", slug: "guides/oauth" },
            { label: "Unix Socket Launcher", slug: "guides/launcher" },
          ],
        },
        {
          label: "Client",
          items: [
            { label: "Transports", slug: "guides/client-transports" },
          ],
        },
        {
          label: "Examples",
          items: [
            { label: "Calculator", slug: "examples/calculator" },
            { label: "Streaming", slug: "examples/streaming" },
            {
              label: "Testing with CLI",
              slug: "examples/testing-with-cli",
            },
          ],
        },
        {
          label: "Reference",
          items: [
            { label: "API", slug: "reference/api" },
            { label: "Configuration", slug: "reference/configuration" },
            { label: "Wire Protocol", slug: "reference/wire-protocol" },
          ],
        },
      ],
    }),
  ],
});
