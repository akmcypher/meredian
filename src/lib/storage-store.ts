import { supabase } from "@/integrations/supabase/client";
import { HttpRangeStore, type ObjectStore } from "./file-ingest";

/** ObjectStore over the app's private `ingest-files` bucket. Reads use signed URLs with HTTP Range; keys are per-user folders (enforced by storage policies). */
export class AppStorageStore implements ObjectStore {
  private bucket = supabase.storage.from("ingest-files");
  private urls = new Map<string, string>();
  rangeReads = 0;
  private async url(key: string) {
    let u = this.urls.get(key);
    if (!u) {
      const { data, error } = await this.bucket.createSignedUrl(key, 3600);
      if (error || !data) throw new Error(`signed URL failed for ${key}: ${error?.message}`);
      u = data.signedUrl;
      this.urls.set(key, u);
    }
    return u;
  }
  private async http(key: string) {
    const u = await this.url(key);
    return new HttpRangeStore(() => u);
  }
  /** Size from object metadata (Content-Range is not exposed to browsers by storage CORS). */
  async size(key: string) {
    const i = key.lastIndexOf("/");
    const { data, error } = await this.bucket.list(key.slice(0, i), {
      search: key.slice(i + 1),
      limit: 100,
    });
    const o = data?.find((x) => x.name === key.slice(i + 1));
    const n = Number((o?.metadata as { size?: number } | null)?.size);
    if (error || !Number.isFinite(n))
      throw new Error(`cannot read size of ${key}: ${error?.message ?? "not found"}`);
    return n;
  }
  async readRange(key: string, s: number, e: number) {
    this.rangeReads++;
    return (await this.http(key)).readRange(key, s, e);
  }
  async put(key: string, bytes: Uint8Array) {
    const { error } = await this.bucket.upload(key, new Blob([bytes as BlobPart]), {
      upsert: true,
      contentType: "application/octet-stream",
    });
    if (error) throw new Error(`upload ${key}: ${error.message}`);
    this.urls.delete(key);
  }
  /** Pass the native Blob to storage; no eager JavaScript whole-file byte copy. */
  async uploadFile(key: string, file: Blob) {
    const { error } = await this.bucket.upload(key, file, {
      upsert: false,
      contentType: "application/octet-stream",
    });
    if (error) throw new Error(`upload ${key}: ${error.message}`);
    this.urls.delete(key);
  }
  async get(key: string) {
    const { data, error } = await this.bucket.download(key);
    if (error || !data) return null;
    return new Uint8Array(await data.arrayBuffer());
  }
}
