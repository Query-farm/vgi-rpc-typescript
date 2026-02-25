import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  site: "https://vgi-rpc-typescript.query.farm",
  integrations: [
    starlight({
      title: "vgi-rpc",
      description:
        "TypeScript RPC server library powered by Apache Arrow IPC.",
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
          href: "https://github.com/rustyconover/vgi-rpc-typescript",
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
          label: "Guides",
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
