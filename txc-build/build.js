// build.js — outputs stop_to_services.json next to this file

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { glob } from "glob";
import { XMLParser } from "fast-xml-parser";
import yauzl from "yauzl";

// Resolve paths relative to THIS FILE, not the CWD
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TXC_DIR = path.join(__dirname, "downloads");          // txc-build/downloads
const OUT_FILE = path.join(__dirname, "stop_to_services.json"); // txc-build/stop_to_services.json

// UK "today" to respect local validity dates
const UK_NOW = new Date().toLocaleString("en-GB", { timeZone: "Europe/London" });
const TODAY = new Date(UK_NOW);

// XML parser
const parser = new XMLParser({ ignoreAttributes:false, attributeNamePrefix:"", trimValues:true });

// atco -> Map<key, svc>
const stopToServices = new Map();
const bestServiceVersion = new Map(); // serviceCode -> { rev, pubTs }

// stats
let filesParsed = 0, servicesSeen = 0, servicesKept = 0, servicesExpired = 0;

const parseISO = s => { if(!s) return null; const d=new Date(String(s)); return isNaN(+d)?null:d; };
const inRange = (t,a,b)=>!(a&&t<a) && !(b&&t>b);

function serviceIsActiveToday(service, txcRoot){
  const op=service?.OperatingPeriod||{};
  const start=parseISO(op.StartDate||op.Start);
  const end  =parseISO(op.EndDate  ||op.End  );

  const vb=txcRoot?.ValidBetween||{};
  const rop=txcRoot?.OperatingPeriod||{};

  const s=start||parseISO(vb.FromDate||vb.StartDate||vb.Start)||parseISO(rop.StartDate||rop.Start);
  const e=end  ||parseISO(vb.ToDate  ||vb.EndDate  ||vb.End  )||parseISO(rop.EndDate  ||rop.End  );

  if(!s && !e) return true; // assume valid if no dates
  return inRange(TODAY, s, e);
}

const pickBest = (s,root)=>({
  rev: Number(s.RevisionNumber ?? root?.RevisionNumber ?? 0) || 0,
  pubTs: parseISO(root?.PublicationTimestamp || root?.CreationDateTime) || new Date(0)
});
const newer = (a,b)=> a.rev!==b.rev ? a.rev>b.rev : a.pubTs>b.pubTs;

function add(atco, svc){
  if(!atco) return;
  let bucket=stopToServices.get(atco);
  if(!bucket){ bucket=new Map(); stopToServices.set(atco,bucket); }
  const key=svc.ref||svc.name||svc.serviceCode||JSON.stringify(svc);
  if(!bucket.has(key)) bucket.set(key,svc);
}

function parseTXC(xml){
  const doc=parser.parse(xml); const txc=doc.TransXChange||doc["TransXChange"]; if(!txc) return;

  const services=Array.isArray(txc.Services?.Service)?txc.Services.Service:(txc.Services?.Service?[txc.Services.Service]:[]);
  const secs=txc.JourneyPatternSections?.JourneyPatternSection;
  const jps=Array.isArray(secs)?secs:(secs?[secs]:[]);
  const sectionStops=new Set();
  for(const sec of jps){
    const links=Array.isArray(sec.JourneyPatternTimingLink)?sec.JourneyPatternTimingLink:[sec.JourneyPatternTimingLink].filter(Boolean);
    for(const link of links){ const f=link.From?.StopPointRef, t=link.To?.StopPointRef;
      if(f) sectionStops.add(String(f).trim()); if(t) sectionStops.add(String(t).trim()); }
  }

  const vjsNode=txc.VehicleJourneys?.VehicleJourney;
  const vjs=Array.isArray(vjsNode)?vjsNode:(vjsNode?[vjsNode]:[]);
  const vjCodes=new Set(vjs.map(vj=>String(vj.ServiceRef||vj.ServiceCode||"").trim()).filter(Boolean));

  for(const s of services){
    servicesSeen++;
    const code=String(s.ServiceCode||"").trim();
    if(vjCodes.size && code && !vjCodes.has(code)) continue; // service not used in this file

    if(!serviceIsActiveToday(s, txc)){ servicesExpired++; continue; }

    if(code){
      const ver=pickBest(s, txc), prev=bestServiceVersion.get(code);
      if(prev && !newer(ver, prev)) continue;
      bestServiceVersion.set(code, ver);
    }

    const line=s.Lines?.Line?.LineName || s.LineName || s.Description || "";
    const ops=[].concat(s.Operators?.Operator||[]).map(o=>(o.OperatorNameOnLicence||o.TradingName||o.OperatorShortName||"").trim()).filter(Boolean);
    const operator=ops[0] || (s.RegisteredOperatorRef||"").trim();
    const refGuess=(s.ServiceRef||s.LineName||line||"").toString();

    const svc={ ref:String(refGuess).trim(), name:String(line||refGuess||"").trim(), operator:String(operator||"").trim(), serviceCode:code };

    for(const atco of sectionStops) add(atco, svc);
    servicesKept++;
  }
}

async function parseZip(zipPath){
  await new Promise((resolve,reject)=>{
    yauzl.open(zipPath,{lazyEntries:true},(err,zip)=>{
      if(err) return reject(err);
      zip.readEntry();
      zip.on("entry",entry=>{
        if(!/\.xml$/i.test(entry.fileName)){ zip.readEntry(); return; }
        zip.openReadStream(entry,(e,stream)=>{
          if(e) return reject(e);
          const chunks=[]; stream.on("data",d=>chunks.push(d));
          stream.on("end",()=>{ try{ parseTXC(Buffer.concat(chunks).toString("utf8")); }catch{} zip.readEntry(); });
        });
      });
      zip.on("end",()=>{ filesParsed++; resolve(); });
      zip.on("error",reject);
    });
  });
}

const zips = await glob(path.join(TXC_DIR, "**/*.zip"));
console.log("TXC zips:", zips.length);
for(const z of zips){ console.log("→", path.basename(z)); await parseZip(z); }

const out={};
for(const [atco,map] of stopToServices.entries()){
  const arr=[...map.values()];
  const num=v=>{ const m=String(v||"").match(/^(\d+)/); return m?+m[1]:NaN; };
  arr.sort((a,b)=>{
    const an=num(a.ref), bn=num(b.ref);
    if(!isNaN(an)&&!isNaN(bn)) return an-bn;
    if(!isNaN(an)) return -1;
    if(!isNaN(bn)) return 1;
    return String(a.ref||a.name).localeCompare(String(b.ref||b.name), undefined, {numeric:true});
  });
  out[atco]=arr;
}
fs.writeFileSync(OUT_FILE, JSON.stringify(out));
console.log("Wrote", OUT_FILE, "stops:", Object.keys(out).length);
console.log("Files parsed:", filesParsed);
console.log("Services seen:", servicesSeen);
console.log("Services kept (active today):", servicesKept);
console.log("Services skipped (expired/not yet valid):", servicesExpired);
