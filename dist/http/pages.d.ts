/**
 * Pre-rendered HTML pages for the vgi-rpc HTTP server.
 * Matches the styling of the Python and Go implementations.
 */
import type { MethodDefinition } from "../types.js";
export declare const LOGO_URL = "https://vgi-rpc-python.query.farm/assets/logo-hero.png";
export declare const FONTS = "<link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">\n<link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>\n<link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap\" rel=\"stylesheet\">";
export declare const ERROR_PAGE_STYLE = "<style>\nbody { font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 600px;\n       margin: 0 auto; padding: 60px 20px 0; color: #2c2c1e; text-align: center;\n       background: #faf8f0; }\n.logo { margin-bottom: 24px; }\n.logo img { width: 120px; height: 120px; border-radius: 50%;\n             box-shadow: 0 4px 24px rgba(0,0,0,0.12); }\nh1 { color: #2d5016; margin-bottom: 8px; font-weight: 700; }\ncode { font-family: 'JetBrains Mono', monospace; background: #f0ece0;\n        padding: 2px 6px; border-radius: 3px; font-size: 0.9em; color: #2c2c1e; }\na { color: #2d5016; text-decoration: none; }\na:hover { color: #4a7c23; }\np { line-height: 1.7; color: #6b6b5a; }\n.detail { margin-top: 12px; padding: 12px 16px; background: #f0ece0;\n           border-radius: 6px; font-size: 0.9em; color: #6b6b5a; }\nfooter { margin-top: 48px; padding: 20px 0; border-top: 1px solid #f0ece0;\n          color: #6b6b5a; font-size: 0.85em; line-height: 1.8; }\nfooter a { color: #2d5016; font-weight: 600; }\nfooter a:hover { color: #4a7c23; }\n</style>";
export declare function buildLandingPage(protocolName: string, serverId: string, describePath: string | null, repoUrl: string | null): string;
export declare function buildNotFoundPage(prefix: string, protocolName: string): string;
export declare function buildDescribePage(protocolName: string, serverId: string, methods: Map<string, MethodDefinition>, repoUrl: string | null): string;
//# sourceMappingURL=pages.d.ts.map