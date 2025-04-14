# Land Job Processing

This codebase is designed to process daily asset movement data based on `asset_id`, analyze trajectory patterns using angular changes, generate cluster polygons, and save results on AWS S3.

---

## Directory Structure

```
project/
│
├── main.py                  # Main orchestration script
├── main_func.py             # Utility functions for calculations
└── polygon_generator.py     # Polygon generation logic using clustering
```

---

## Overview

The application performs the following tasks:
- Processes asset location data from S3
- Identifies turning points based on angular deviation
- Clusters GPS data to generate movement polygons
- Calculates metrics (area, distance, duration)
- Uploads the result as a Parquet file to S3

---

## main.py

### Orchestration Flow:

1. **Logging Setup**  
   Initializes logging to capture processing flow and errors.

2. **Load Parquet File**  
   Retrieves the file from the S3 path based on *yesterday's UTC date*.

3. **Extract Sample Asset**  
   Picks a sample asset ID (16th unique asset) for analysis.

4. **Polygon Generation**  
   Uses angular turns and clustering logic to derive movement polygons.

5. **Metrics Calculation**  
   Computes area, perimeter (distance), and duration for each polygon.

6. **Compile & Save Output**  
   - Formats records
   - Saves locally as Parquet
   - Uploads to S3

### Functions Used:

- `generate_cluster_polygons_for_asset`
- `calculate_polygon_area`
- `calculate_distance`
- `calculate_duration`
- `create_geojson_polygons_only`
- `convert_to_pure_json`

---

## main_func.py

Utility functions used throughout the pipeline.

### Functions:

- `convert_to_pure_json(geojson_str)`  
  Parses GeoJSON-like strings into valid JSON.

- `create_geojson_polygons_only(polygons_df)`  
  Converts a DataFrame with polygons into valid GeoJSON format.

- `calculate_polygon_area(polygon)`  
  Returns polygon area in **hectares** using Shapely.

- `calculate_distance(polygon)`  
  Computes polygon perimeter using **geopy**’s geodesic method.

- `calculate_duration(polygon, original_df)`  
  Duration between first and last GPS points in a polygon.

---

## polygon_generator.py

Handles clustering and polygon generation.

### Key Functions:

- `calculate_angle(lat1, lon1, lat2, lon2, lat3, lon3)`  
  Calculates turning angle across three GPS points.

- `generate_nearby_points(lat, lon, num_points=500, radius_meters=5)`  
  Creates artificial nearby points to improve clustering.

- `generate_cluster_polygons_for_asset(...)`  
  Main polygon generation routine:
  - Resamples to 2-minute intervals
  - Identifies sharp turning points
  - Generates nearby points
  - Applies **DBSCAN clustering**
  - Applies **convex hull** for each cluster

### Returns:

- `polygons_df`: Polygons with metadata  
- `combined_df`: Clustered GPS points  
- `df`: Resampled GPS data

---

## Output Format

Parquet file with the following fields:

- `asset_id`
- `polygon_geojson`
- `area`
- `distance`
- `duration`
- `start_time`
- `end_time`
- and more...

---

## Running the Code on EC2

### Setup Environment:

```bash
sudo apt update
sudo apt install python3-pip -y
python3 -m pip --version
sudo apt install python3 python3-venv -y
python3 -m venv my_env
source my_env/bin/activate
```

### Install Dependencies:

```bash
pip install pandas numpy matplotlib shapely geopandas geopy pyarrow s3fs scikit-learn scipy
```

---

## Logs & Output

- Logs are saved in: `land_job_processing.log`  
- Output Parquet is named: `Landjob_YYYY-MM-DD.parquet`

---

## AWS Setup

- Ensure proper **IAM role or AWS credentials** with S3 read/write access.
- Expected file naming and folder structure must be followed.

---

## Future Improvements

- Loop through **all asset IDs** dynamically (currently disabled due to production issues).
- **Dockerize** the application for simplified deployment.

---