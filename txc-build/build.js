import fs from "node:fs";
import path from "node:path";
import { glob } from "glob";
import { XMLParser } from "fast-xml-parser";
import yauzl from "yauzl";

// === CONFIG ===
const TXC_DIR = "./txc-build/downloads";
const OUT_FILE = "./txc-build/stop_to_services.json";
const TODAY = new Date(); // Use runner's date; you can pin to UK if you want.

// === XML parser ===
const parser = new XMLParser({ ignoreAttributes:false, attributeNamePrefix:"", trimValues:true });

// atco -> Map<key, svc>
const stopToServices = new Map();

// Track best version per serviceCode (so older revisions don't override newer ones)
const bestServiceVersion = new Map(); // serviceCode -> { rev, pubTs }

// stats
let filesParsed = 0;
let servicesSeen = 0;
let servicesKept = 0;
let servicesExpired = 0;

// ---------- helpers ----------
const parseISO = (s) => {
  if (!s) return null;
  const d = new Date(String(s));
  return isNaN(+d) ? null : d;
};

function inRange(today, start, end) {
  if (start && today < start) return false;
  if (end && today > end) return false;
  return true;
}

function serviceIsActiveToday(service, txcRoot) {
  // Prefer Service.OperatingPeriod { StartDate, EndDate }
  const op = service?.OperatingPeriod || {};
  const start = parseISO(op.StartDate || op.Start || service?.OperatingPeriodStartDate);
  const end   = parseISO(op.EndDate || op.End || service?.OperatingPeriodEndDate);

  // If Service has no dates, try some common fallbacks found in TXC
  // (different publishers place date ranges in various places)
  let fallbackStart = null, fallbackEnd = null;

  // Sometimes TXC root has ValidBetween/OperatingPeriod
  const vb = txcRoot?.ValidBetween || {};
  fallbackStart = fallbackStart || parseISO(vb.FromDate || vb.StartDate || vb.Start);
  fallbackEnd   = fallbackEnd   || parseISO(vb.ToDate   || vb.EndDate   || vb.End);

  const rop = txcRoot?.OperatingPeriod || {};
  fallbackStart = fallbackStart || parseISO(rop.StartDate || rop.Start);
  fallbackEnd   = fallbackEnd   || parseISO(rop.EndDate   || rop.End);

  const sStart = start || fallbackStart;
  const sEnd   = end   || fallbackEnd;

  // If no dates anywhere, we assume it's valid (publishers sometimes omit dates)
  if (!sStart && !sEnd) return true;

  return inRange(TODAY, sStart, sEnd);
}

function pickBestServiceVersion(service, txcRoot) {
  // Determine a version tuple for this service:
  // 1) Service.RevisionNumber (numeric, higher is newer), else
  // 2) TXC root PublicationTimestamp (latest is newer)
  const rev = Number(service.RevisionNumber ?? service.Revision ?? txcRoot?.RevisionNumber ?? 0);
  const pubTs = parseISO(txcRoot?.PublicationTimestamp || txcRoot?.CreationDateTime) || new Date(0);
  return { rev: isNaN(rev) ? 0 : rev, pubTs };
}

function isNewerVersion(a, b) {
  // compare by revision; if equal, compare by publication timestamp
  if (a.rev !== b.rev) return a.rev > b.rev;
  return a.pubTs > b.pubTs;
}

function add(atco, svc) {
  if (!atco) return;
  let bucket = stopToServices.get(atco);
  if (!bucket) { bucket = new Map(); stopToServices.set(atco, bucket); }
  const key = svc.ref || svc.name || svc.serviceCode || JSON.stringify(svc);
  if (!bucket.has(key)) bucket.set(key, svc);
}

// ---------- core parsing ----------
function parseTXC(xml) {
  const doc = parser.parse(xml);
  const txc = doc.TransXChange || doc["TransXChange"];
  if (!txc) return;

  // Services
  const services = Array.isArray(txc.Services?.Service)
    ? txc.Services.Service
    : txc.Services?.Service ? [txc.Services.Service] : [];

  // JourneyPatternSections → gather StopPointRefs
  const sections = txc.JourneyPatternSections?.JourneyPatternSection;
  const jps = Array.isArray(sections) ? sections : sections ? [sections] : [];
  const sectionStops = new Set();
  for (const sec of jps) {
    const links = Array.isArray(sec.JourneyPatternTimingLink) ? sec.JourneyPatternTimingLink : [sec.JourneyPatternTimingLink].filter(Boolean);
    for (const link of links) {
      const from = link.From?.StopPointRef, to = link.To?.StopPointRef;
      if (from) sectionStops.add(String(from).trim());
      if (to)   sectionStops.add(String(to).trim());
    }
  }

  // VehicleJourneys (sometimes needed to confirm a serviceCode appears in this file)
  const vjsNode = txc.VehicleJourneys?.VehicleJourney;
  const vjs = Array.isArray(vjsNode) ? vjsNode : vjsNode ? [vjsNode] : [];
  const vjServiceCodes = new Set(
    vjs.map(vj => String(vj.ServiceRef || vj.ServiceCode || "").trim()).filter(Boolean)
  );

  for (const s of services) {
    servicesSeen++;

    const serviceCode = String(s.ServiceCode || "").trim();
    // If the file has VJs, ensure this Service actually appears
    if (vjServiceCodes.size && serviceCode && !vjServiceCodes.has(serviceCode)) {
      continue;
    }

    // validity filter
    const active = serviceIsActiveToday(s, txc);
    if (!active) { servicesExpired++; continue; }

    // version filter
    if (serviceCode) {
      const ver = pickBestServiceVersion(s, txc);
      const prev = bestServiceVersion.get(serviceCode);
      if (prev && !isNewerVersion(ver, prev)) {
        // older version; skip adding this service to avoid stale data
        continue;
      }
      bestServiceVersion.set(serviceCode, ver);
    }

    // Build display info
    const lineName = s.Lines?.Line?.LineName || s.LineName || s.Description || "";
    const operators = []
      .concat(s.Operators?.Operator || [])
      .map(o => (o.OperatorNameOnLicence || o.TradingName || o.OperatorShortName || "").trim())
      .filter(Boolean);
    const operator = operators[0] || (s.RegisteredOperatorRef || "").trim();
    const refGuess = (s.ServiceRef || s.LineName || lineName || "").toString();

    const svcLite = {
      ref: String(refGuess).trim(),
      name: String(lineName || refGuess || "").trim(),
      operator: String(operator || "").trim(),
      serviceCode: serviceCode
    };

    // Add to all stops we saw in the JP sections (coarse but accurate enough)
    for (const atco of sectionStops) add(atco, svcLite);
    servicesKept++;
  }
}

async function parseZip(zipPath) {
  await new Promise((resolve, reject) => {
    yauzl.open(zipPath, { lazyEntries:true }, (err, zip) => {
      if (err) return reject(err);
      zip.readEntry();
      zip.on("entry", entry => {
        if (!/\.xml$/i.test(entry.fileName)) {
          zip.readEntry();
          return;
        }
        zip.openReadStream(entry, (err2, stream) => {
          if (err2) return reject(err2);
          const chunks=[]; stream.on("data",d=>chunks.push(d));
          stream.on("end", ()=>{
            try { parseTXC(Buffer.concat(chunks).toString("utf8")); }
            catch(e){ console.warn("Bad XML", zipPath, entry.fileName, e.message); }
            zip.readEntry();
          });
        });
      });
      zip.on("end", ()=>{ filesParsed++; resolve(); });
      zip.on("error", reject);
    });
  });
}

// ---------- main ----------
const zips = await glob(path.join(TXC_DIR, "**/*.zip"));
console.log("TXC zips:", zips.length);
for (const z of zips) {
  console.log("→", path.basename(z));
  await parseZip(z);
}

// Emit compact JSON: atco -> array of services (sorted)
const out = {};
for (const [atco, map] of stopToServices.entries()) {
  const arr = [...map.values()];
  // Sort services: numeric refs first, then lexical
  const num = v => { const m = String(v||"").match(/^(\d+)/); return m ? +m[1] : NaN; };
  arr.sort((a,b)=>{
    const an=num(a.ref), bn=num(b.ref);
    if(!isNaN(an)&&!isNaN(bn)) return an-bn;
    if(!isNaN(an)) return -1;
    if(!isNaN(bn)) return 1;
    return String(a.ref||a.name).localeCompare(String(b.ref||b.name), undefined, {numeric:true});
  });
  out[atco] = arr;
}
fs.writeFileSync(OUT_FILE, JSON.stringify(out));
console.log("Wrote", OUT_FILE, "stops:", Object.keys(out).length);

// summary
console.log("Files parsed:", filesParsed);
console.log("Services seen:", servicesSeen);
console.log("Services kept (active today):", servicesKept);
console.log("Services skipped (expired/not yet valid):", servicesExpired);
