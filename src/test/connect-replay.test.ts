// @vitest-environment node
import { afterEach, expect, it, vi } from "vitest";
import type { SupabaseClient } from "@supabase/supabase-js";
import { hydrateReplayClaim } from "@/lib/connect-agent.server";

afterEach(() => vi.unstubAllGlobals());

it("defers a letter before its UTF-8 payload exceeds the claim budget", async () => {
  const position = "\u754c".repeat(20);
  const letter = (position: string) =>
    JSON.stringify({
      type: "DEAD_LETTER",
      dead_letter: {
        stream: "topic",
        namespace: "kafka",
        position,
        error_code: "invalid_record",
        payload: { value_b64: "eA==" },
      },
    });
  const body = [letter(position), letter("0:2")].join("\n");
  const claim = {
    run_id: "run",
    lease_token: "lease",
    operation: "replay",
    streams: [
      {
        id: "stream",
        dead_letters: [
          { id: "first", position, object_key: "object", line: 0 },
          { id: "second", position: "0:2", object_key: "object", line: 1 },
        ],
      },
    ],
  };
  const released: string[][] = [];
  const db = {
    storage: {
      from: () => ({
        createSignedUrl: async () => ({ data: { signedUrl: "https://storage.test/object" } }),
      }),
    },
    rpc: async (_name: string, args: { _ids: string[] }) => {
      released.push(args._ids);
      return { data: 1, error: null };
    },
  } as unknown as SupabaseClient;
  vi.stubGlobal("fetch", async () => new Response(body));
  await hydrateReplayClaim(claim, db, 200);
  expect(claim.streams[0]!.dead_letters.map((x) => x.id)).toEqual(["first"]);
  expect(released).toEqual([["second"]]);
});

it("cancels an oversized object stream before parsing its remainder", async () => {
  let cancelled = false;
  const claim = {
    run_id: "run",
    lease_token: "lease",
    operation: "replay",
    streams: [
      {
        id: "stream",
        dead_letters: [{ id: "letter", position: "0:1", object_key: "object", line: 0 }],
      },
    ],
  };
  const db = {
    storage: {
      from: () => ({
        createSignedUrl: async () => ({ data: { signedUrl: "https://storage.test/object" } }),
      }),
    },
  } as unknown as SupabaseClient;
  vi.stubGlobal(
    "fetch",
    async () =>
      new Response(
        new ReadableStream({
          pull(controller) {
            controller.enqueue(new Uint8Array(1024 * 1024));
          },
          cancel() {
            cancelled = true;
          },
        }),
      ),
  );
  await expect(hydrateReplayClaim(claim, db)).rejects.toMatchObject({ status: 413 });
  expect(cancelled).toBe(true);
});
