import fs from "node:fs";
import path from "node:path";
import { glob } from "glob";
import { XMLParser } from "fast-xml-parser";
import yauzl from "yauzl";

const TXC_DIR = "./txc-build/downloads";
const OUT_FILE = "./txc-build/stop_to_services.json";

const parser = new XMLParser({ ignoreAttributes:false, attributeNamePrefix:"", trimValues:true });
const stopToServices = new Map(); // atco -> Map<key,svc>

function add(atco, svc) {
  if (!atco) return;
  let bucket = stopToServices.get(atco);
  if (!bucket) { bucket = new Map(); stopToServices.set(atco, bucket); }
  const key = svc.ref || svc.name || svc.serviceCode || JSON.stringify(svc);
  if (!bucket.has(key)) bucket.set(key, svc);
}

function parseTXC(xml) {
  const doc = parser.parse(xml);
  const txc = doc.TransXChange || doc["TransXChange"]; if (!txc) return;

  const services = Array.isArray(txc.Services?.Service)
    ? txc.Services.Service
    : txc.Services?.Service ? [txc.Services.Service] : [];

  const svcInfo = new Map();
  for (const s of services) {
    const serviceCode = s.ServiceCode || "";
    const lineName = s.Lines?.Line?.LineName || s.LineName || s.Description || "";
    const operators = []
      .concat(s.Operators?.Operator || [])
      .map(o => (o.OperatorNameOnLicence || o.TradingName || o.OperatorShortName || "").trim())
      .filter(Boolean);
    const operator = operators[0] || (s.RegisteredOperatorRef || "").trim();
    const refGuess = (s.ServiceRef || s.LineName || lineName || "").toString();
    svcInfo.set(serviceCode, {
      ref: String(refGuess).trim(),
      name: String(lineName || refGuess || "").trim(),
      operator: String(operator || "").trim(),
      serviceCode: String(serviceCode || "").trim()
    });
  }

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

  const vjsNode = txc.VehicleJourneys?.VehicleJourney;
  const vjs = Array.isArray(vjsNode) ? vjsNode : vjsNode ? [vjsNode] : [];
  for (const vj of vjs) {
    const sc = String(vj.ServiceRef || vj.ServiceCode || "").trim();
    const svc = svcInfo.get(sc); if (!svc) continue;
    for (const atco of sectionStops) add(atco, svc);
  }
}

async function parseZip(zipPath) {
  await new Promise((resolve, reject) => {
    yauzl.open(zipPath, { lazyEntries:true }, (err, zip) => {
      if (err) return reject(err);
      zip.readEntry();
      zip.on("entry", entry => {
        if (!/\.xml$/i.test(entry.fileName)) return zip.readEntry();
        zip.openReadStream(entry, (err2, stream) => {
          if (err2) return reject(err2);
          const chunks=[]; stream.on("data",d=>chunks.push(d));
          stream.on("end", ()=>{ try{ parseTXC(Buffer.concat(chunks).toString("utf8")); } catch {} zip.readEntry(); });
        });
      });
      zip.on("end", resolve);
      zip.on("error", reject);
    });
  });
}

const zips = await glob(path.join(TXC_DIR, "**/*.zip"));
console.log("TXC zips:", zips.length);
for (const z of zips) { console.log("→", path.basename(z)); await parseZip(z); }

const out = {};
for (const [atco, map] of stopToServices.entries()) {
  const arr = [...map.values()];
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
