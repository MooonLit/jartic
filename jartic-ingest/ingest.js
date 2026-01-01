import fetch from "node-fetch";
import pkg from "pg";
const { Pool } = pkg;

// Supabase connection string from GitHub Secrets
const pool = new Pool({
  connectionString: process.env.SUPABASE_DB_URL
});

// Replace this with the JARTIC API endpoint you need
const JARTIC_URL = "https://api.jartic-open-traffic.org/geoserver/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=t_travospublic_measure_5m&srsName=EPSG:4326&outputFormat=application/json";

async function ingest() {
  try {
    console.log("Fetching JARTIC data...");
    const res = await fetch(JARTIC_URL);
    const data = await res.json();

    for (const feature of data.features) {
      const p = feature.properties;
      const coords = feature.geometry.coordinates;

      await pool.query(
        `INSERT INTO jartic_traffic (
          observation_code,
          road_type,
          dataset_type,
          time_code,
          observed_at,
          volume_up,
          volume_down,
          small_vehicle_count,
          large_vehicle_count,
          total_volume,
          geom,
          raw_properties
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,ST_SetSRID(ST_MakePoint($11,$12),4326),$13
        )
        ON CONFLICT (observation_code,time_code,dataset_type)
        DO UPDATE SET
          volume_up = EXCLUDED.volume_up,
          volume_down = EXCLUDED.volume_down,
          small_vehicle_count = EXCLUDED.small_vehicle_count,
          large_vehicle_count = EXCLUDED.large_vehicle_count,
          total_volume = EXCLUDED.total_volume,
          geom = EXCLUDED.geom,
          raw_properties = EXCLUDED.raw_properties
        `,
        [
          p["常時観測点コード"],
          p["道路種別"],
          "様式1",                     
          p["時間コード"],
          new Date(
            p["時間コード"].toString().replace(
              /(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})/,
              "$1-$2-$3T$4:$5:00+09:00"
            )
          ),
          p["上り交通量"] || 0,
          p["下り交通量"] || 0,
          p["小型車"] || 0,
          p["大型車"] || 0,
          (p["上り交通量"] || 0) + (p["下り交通量"] || 0),
          coords[0],  
          coords[1],  
          p            
        ]
      );
    }

    console.log("Ingestion complete!");
  } catch (err) {
    console.error("Error ingesting JARTIC data:", err);
  } finally {
    await pool.end();
  }
}

ingest();