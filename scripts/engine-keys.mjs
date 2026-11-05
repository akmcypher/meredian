// Prints a new Ed25519 key pair for engine tokens.
// ENGINE_TOKEN_SIGNING_KEY goes to the control plane's secret store only; the public key and
// the engine's id (from the Engines page) go to each DataFusion service as
// MERIDIAN_ENGINE_PUBLIC_KEY and MERIDIAN_ENGINE_ID. Nothing is written to disk.
import { generateKeyPairSync } from "node:crypto";

const { privateKey, publicKey } = generateKeyPairSync("ed25519");
console.log(
  `ENGINE_TOKEN_SIGNING_KEY=${privateKey.export({ type: "pkcs8", format: "der" }).toString("base64")}`,
);
console.log(`MERIDIAN_ENGINE_PUBLIC_KEY=${publicKey.export({ format: "jwk" }).x}`);
