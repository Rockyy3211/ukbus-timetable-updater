// operator-resolver.js
// Helpers for mapping raw operator codes → nice display names

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = path.join(__dirname, "data");
const NOC_CSV = path.join(DATA_DIR, "noc.csv");
const OVERRIDES_FILE = path.join(DATA_DIR, "operator-overrides.json");

// Tiny CSV splitter that handles "quoted, values"
function splitCsvLine(line) {
  const result = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        // escaped quote ("")
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      result.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  result.push(cur);
  return result;
}

/**
 * Build a Map<NOC/code, operatorPublicName> from noc.csv
 */
export async function buildNocMap() {
  const map = new Map();

  try {
    const csv = fs.readFileSync(NOC_CSV, "utf8");
    const lines = csv.split(/\r?\n/).filter((l) => l.trim().length);

    if (!lines.length) return map;

    const header = splitCsvLine(lines[0]).map((h) => h.trim());
    const codeIdx = header.findIndex((h) => /^noc.?code$/i.test(h));
    const nameIdx = header.findIndex(
      (h) =>
        /operator.*public.*name/i.test(h) ||
        /operator.*name/i.test(h)
    );

    if (codeIdx === -1 || nameIdx === -1) {
      console.warn(
        "[operators] Could not find nocCode/operatorPublicName columns in",
        NOC_CSV
      );
      return map;
    }

    for (let i = 1; i < lines.length; i++) {
      const row = splitCsvLine(lines[i]);
      if (!row.length) continue;
      const code = (row[codeIdx] || "").trim();
      const name = (row[nameIdx] || "").trim();
      if (!code || !name) continue;
      map.set(code, name);
    }

    console.log("[operators] Loaded NOC entries:", map.size);
  } catch (err) {
    if (err.code === "ENOENT") {
      console.warn("[operators] NOC CSV not found at", NOC_CSV);
    } else {
      console.warn("[operators] Failed to read NOC CSV:", err.message);
    }
  }

  return map;
}

/**
 * Load manual operator overrides from operator-overrides.json
 * Shape: { "TKT_OID": "Bee Network Metroline", ... }
 */
export async function loadOperatorOverrides() {
  try {
    const raw = fs.readFileSync(OVERRIDES_FILE, "utf8");
    const json = JSON.parse(raw);
    if (!json || typeof json !== "object") return {};
    console.log("[operators] Loaded overrides:", Object.keys(json).length);
    return json;
  } catch (err) {
    if (err.code !== "ENOENT") {
      console.warn("[operators] Could not read overrides:", err.message);
    }
    return {};
  }
}

/**
 * Decide on the best display name for an operator given:
 *   - rawCode: NOC / internal code / TKT_OID…
 *   - txcName: name from the TXC Operator element
 *   - nocMap: Map<code, publicName>
 *   - overrides: { codeOrPrefix -> nicer name }
 */
export function resolveOperator(rawCode, txcName, nocMap, overrides) {
  const code = (rawCode || "").trim();
  const name = (txcName || "").trim();
  const ov = overrides || {};

  // 1) Exact override by code
  if (code && Object.prototype.hasOwnProperty.call(ov, code)) {
    return ov[code];
  }

  // 2) Prefix override (e.g. "TKT_OID" matches "TKT_OID:1234")
  if (code) {
    for (const [k, v] of Object.entries(ov)) {
      if (k && code.startsWith(k)) return v;
    }
  }

  // 3) NOC lookup
  if (code && nocMap && nocMap.has(code)) {
    return nocMap.get(code);
  }

  // 4) Override by TXC name
  if (name && Object.prototype.hasOwnProperty.call(ov, name)) {
    return ov[name];
  }

  // 5) Fall back to TXC operator name if present
  if (name) return name;

  // 6) Last resort: show the code
  return code || "";
}
