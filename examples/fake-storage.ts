// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Adapter for the conformance fake object store.
 *
 * Speaks the four-endpoint contract documented in
 * `vgi_rpc.conformance.fake_storage` (POST /alloc, PUT /blob/{id}, GET
 * /blob/{id}, GET /_stats). Shared by every conformance worker that
 * externalizes — the HTTP one and the byte-stream one — so a fixture cannot
 * be wired against a second, subtly different implementation of the same
 * four calls.
 */
import type { ExternalStorage, UploadUrl, UploadUrlProvider } from "../src/external.js";

interface Allocation {
  object_url: string;
  upload_url?: string;
  download_url?: string;
}

export class FakeStorage implements ExternalStorage, UploadUrlProvider {
  constructor(private readonly baseUrl: string) {}

  private async alloc(contentEncoding = ""): Promise<Allocation> {
    const response = await fetch(`${this.baseUrl}/alloc`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: contentEncoding ? JSON.stringify({ content_encoding: contentEncoding }) : "{}",
    });
    if (!response.ok) {
      throw new Error(`fake-storage /alloc failed: ${response.status}`);
    }
    return (await response.json()) as Allocation;
  }

  async upload(data: Uint8Array, contentEncoding: string): Promise<string> {
    const allocation = await this.alloc(contentEncoding);
    const uploadUrl = allocation.upload_url ?? allocation.object_url;
    const downloadUrl = allocation.download_url ?? allocation.object_url;

    const putHeaders: Record<string, string> = { "Content-Type": "application/octet-stream" };
    if (contentEncoding) putHeaders["Content-Encoding"] = contentEncoding;
    const putResp = await fetch(uploadUrl, { method: "PUT", headers: putHeaders, body: data });
    if (!putResp.ok) {
      throw new Error(`fake-storage PUT failed: ${putResp.status}`);
    }
    return downloadUrl;
  }

  async generateUploadUrl(): Promise<UploadUrl> {
    const allocation = await this.alloc();
    return {
      uploadUrl: allocation.upload_url ?? allocation.object_url,
      downloadUrl: allocation.download_url ?? allocation.object_url,
      expiresAt: new Date(Date.now() + 3600_000),
    };
  }
}
