import fs from "node:fs";
import path from "node:path";

const IN = "./txc-build/stop_to_services.json";
const OUT_DIR = "./txc-build/shards";
fs.mkdirSync(OUT_DIR, { recursive: true });

const src = JSON.parse(fs.readFileSync(IN, "utf8"));
const buckets = new Map(); // prefix -> { atco: services[] }

for (const [atco, list] of Object.entries(src)) {
  const prefix = atco.slice(0, 4); // e.g. "1800"
  if (!buckets.has(prefix)) buckets.set(prefix, {});
  buckets.get(prefix)[atco] = list;
}

for (const [prefix, obj] of buckets.entries()) {
  const file = path.join(OUT_DIR, `${prefix}.json`);
  fs.writeFileSync(file, JSON.stringify(obj));
  console.log("shard", file, Object.keys(obj).length);
}
