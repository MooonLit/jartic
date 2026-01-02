import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// Read environment variables
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error("❌ SUPABASE_URL or SUPABASE_ANON_KEY not set in environment");
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// Full Japan bounding box
const BBOX = [129.0, 31.0, 146.0, 46.0];
const DATASET_TYPE = "様式1";

// Generate JST 5-minute interval code
function generateTimeCode(offsetMinutes = 0) {
  const now = new Date();
  const jstTime = new Date(now.getTime() + 9*60*60000 - offsetMinutes*60000);
  const minutes = Math.floor(jstTime.getMinutes() / 5) * 5;
  jstTime.setMinutes(minutes, 0, 0);
  return jstTime.toISOString().slice(0,16).replace(/[-:T]/g,"").slice(0,12);
}

// Region fallback
function getRegionFromCoordinates(lat, lng) {
  if (lat >= 35.5 && lat <= 36.0 && lng >= 139.5 && lng <= 140.0) return "Tokyo";
  if (lat >= 34.5 && lat <= 35.5 && lng >= 135.0 && lng <= 136.0) return "Osaka";
  if (lat >= 35.0 && lat <= 36.0 && lng >= 136.5 && lng <= 137.5) return "Nagoya";
  return `Station at ${lat.toFixed(2)}°N, ${lng.toFixed(2)}°E`;
}

// Fetch JARTIC data
async function fetchTrafficData() {
  for (let i=0;i<12;i++) {
    const timeCode = generateTimeCode(i*5);
    const params = new URLSearchParams({
      service: "WFS",
      version: "2.0.0",
      request: "GetFeature",
      typeNames: "t_travospublic_measure_5m",
      srsName: "EPSG:4326",
      outputFormat: "application/json",
      maxFeatures: "1000",
      cql_filter: `時間コード=${timeCode} AND BBOX(ジオメトリ,${BBOX},'EPSG:4326')`
    });
    const url = `https://api.jartic-open-traffic.org/geoserver?${params}`;
    console.log(`[INFO] Trying time code: ${timeCode}`);
    try {
      const res = await fetch(url, { headers: { Accept: "application/json" }});
      const data = await res.json();
      if (data.features && data.features.length > 0) return {data, timeCode};
      console.log(`[INFO] No data for ${timeCode}`);
    } catch(e) { console.log(`[ERROR] Fetch failed for ${timeCode}:`, e); }
  }
  throw new Error("No data available for recent time codes");
}

// Insert into Supabase
async function ingest() {
  const { data, timeCode } = await fetchTrafficData();
  console.log(`[INFO] Processing ${data.features.length} stations`);
  
  const rows = data.features.map((f,i)=>{
    const props = f.properties;
    const coords = f.geometry.coordinates;
    let lng, lat;
    if (typeof coords[0]==="number") { lng=coords[0]; lat=coords[1]; } 
    else { lng=coords[0][0]; lat=coords[0][1]; }
    const upSmall = props["上り・小型交通量"]||0;
    const upLarge = props["上り・大型交通量"]||0;
    const downSmall = props["下り・小型交通量"]||0;
    const downLarge = props["下り・大型交通量"]||0;
    return {
      observation_code: props["常時観測点コード"]||`station_${i}`,
      road_type: props["道路種別"]||null,
      dataset_type: DATASET_TYPE,
      time_code: timeCode,
      observed_at: new Date(),
      volume_up: upSmall+upLarge,
      volume_down: downSmall+downLarge,
      small_vehicle_count: upSmall+downSmall,
      large_vehicle_count: upLarge+downLarge,
      total_volume: upSmall+upLarge+downSmall+downLarge,
      lat, lng,
      raw_properties: props,
      region: getRegionFromCoordinates(lat,lng)
    }
  }).filter(Boolean);

  // Insert in batches
  const BATCH_SIZE = 20;
  for (let i=0;i<rows.length;i+=BATCH_SIZE){
    const batch = rows.slice(i,i+BATCH_SIZE);
    const {error} = await supabase.from("jartic_traffic").upsert(batch,{
      onConflict: ["observation_code","time_code","dataset_type"]
    });
    if(error) console.error("[ERROR] Batch insert failed:", error);
    else console.log(`[INFO] Inserted batch ${i/BATCH_SIZE+1}/${Math.ceil(rows.length/BATCH_SIZE)}`);
    await new Promise(res=>setTimeout(res,200));
  }
  console.log(`[INFO] Ingestion complete`);
}

ingest();