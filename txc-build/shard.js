// shard.js — reads stop_to_services.json next to this file and writes shards/XXXX.json

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const IN = path.join(__dirname, "stop_to_services.json");
const OUT_DIR = path.join(__dirname, "shards");

if (!fs.existsSync(IN)) {
  console.error("ERROR: Missing", IN, "(did build.js produce it?)");
  process.exit(1);
}

fs.mkdirSync(OUT_DIR, { recursive: true });

const src = JSON.parse(fs.readFileSync(IN, "utf8"));
const buckets = new Map(); // prefix -> { atco: services[] }

for (const [atco, list] of Object.entries(src)) {
  const prefix = String(atco).slice(0, 4) || "misc";
  if (!buckets.has(prefix)) buckets.set(prefix, {});
  buckets.get(prefix)[atco] = list;
}

let total = 0;
for (const [prefix, obj] of buckets.entries()) {
  const file = path.join(OUT_DIR, `${prefix}.json`);
  fs.writeFileSync(file, JSON.stringify(obj));
  const count = Object.keys(obj).length;
  total += count;
  console.log(`shard ${path.basename(file)} - keys: ${count}`);
}

console.log("Total keys sharded:", total);
