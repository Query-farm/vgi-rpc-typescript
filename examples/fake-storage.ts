// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Adapter for the conformance fake object store.
 *
 * Speaks the contract documented in `vgi_rpc.conformance.fake_storage`
 * (POST /alloc, PUT /blob/{id}, GET /blob/{id}, GET /_stats), plus its object
 * API (`ObjectStore`: an allocation with `purpose: "object"` is a
 * `putObject`, `GET /objects/{id}` is `get`). Shared by every conformance worker that
 * externalizes — the HTTP one and the byte-stream one — so a fixture cannot
 * be wired against a second, subtly different implementation of the same
 * four calls.
 */
import {
  type ExternalStorage,
  ObjectMissingError,
  type ObjectStore,
  type UploadUrl,
  type UploadUrlProvider,
} from "../src/external.js";

interface Allocation {
  object_id: string;
  object_url: string;
  upload_url?: string;
  download_url?: string;
  upload_headers?: Record<string, string>;
}

function hexToBase64(hex: string): string {
  let binary = "";
  for (let i = 0; i < hex.length; i += 2) binary += String.fromCharCode(Number.parseInt(hex.slice(i, i + 2), 16));
  return btoa(binary);
}

async function sha256Base64(data: Uint8Array): Promise<string> {
  const buf = new ArrayBuffer(data.byteLength);
  new Uint8Array(buf).set(data);
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", buf));
  let binary = "";
  for (const b of digest) binary += String.fromCharCode(b);
  return btoa(binary);
}

export class FakeStorage implements ExternalStorage, UploadUrlProvider, ObjectStore {
  constructor(private readonly baseUrl: string) {}

  private async alloc(request: Record<string, unknown> = {}): Promise<Allocation> {
    const response = await fetch(`${this.baseUrl}/alloc`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    });
    if (!response.ok) {
      throw new Error(`fake-storage /alloc failed: ${response.status}`);
    }
    return (await response.json()) as Allocation;
  }

  async upload(data: Uint8Array, contentEncoding: string): Promise<string> {
    const allocation = await this.alloc(contentEncoding ? { content_encoding: contentEncoding } : {});
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

  /** A sized allocation (upload-URL v2): the store verifies the PUT's length
   *  and `x-amz-checksum-sha256` like S3/R2 and refuses any other body. */
  async generateUploadUrl(size: number, sha256Hex: string): Promise<UploadUrl> {
    const allocation = await this.alloc({ size, sha256_b64: hexToBase64(sha256Hex) });
    return {
      uploadUrl: allocation.upload_url ?? allocation.object_url,
      downloadUrl: allocation.download_url ?? allocation.object_url,
      expiresAt: new Date(Date.now() + 3600_000),
      headers: { ...(allocation.upload_headers ?? {}) },
      objectId: String(allocation.object_id),
    };
  }

  /** A sized object allocation (counted `object_puts`) and a verified PUT. */
  async putObject(data: Uint8Array): Promise<string> {
    const allocation = await this.alloc({
      size: data.byteLength,
      sha256_b64: await sha256Base64(data),
      purpose: "object",
    });
    const headers: Record<string, string> = {
      "Content-Type": "application/octet-stream",
      ...(allocation.upload_headers ?? {}),
    };
    const put = await fetch(allocation.upload_url ?? allocation.object_url, { method: "PUT", headers, body: data });
    if (!put.ok) throw new Error(`fake-storage PUT failed: ${put.status}`);
    return String(allocation.object_id);
  }

  /** `GET /objects/{id}` (counted `object_gets`); 404 is a missing object. */
  async get(objectId: string, options: { maxBytes?: number } = {}): Promise<Uint8Array> {
    const response = await fetch(`${this.baseUrl}/objects/${encodeURIComponent(objectId)}`);
    if (response.status === 404) {
      await response.body?.cancel().catch(() => {});
      throw new ObjectMissingError("fake storage object is gone");
    }
    if (!response.ok) throw new Error(`fake-storage object GET failed: ${response.status}`);
    const data = new Uint8Array(await response.arrayBuffer());
    if (options.maxBytes != null && data.byteLength > options.maxBytes) {
      throw new Error(`object is ${data.byteLength} bytes, over max_fetch_bytes (${options.maxBytes})`);
    }
    return data;
  }
}
