import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Polygon
import geopandas as gpd

    
from main_func import calculate_distance, calculate_duration, create_geojson_polygons_only, calculate_polygon_area, convert_to_pure_json
from polygon_generator import generate_cluster_polygons_for_asset

# Set up logging
logging.basicConfig(
    filename='land_job_processing.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def main():
    logging.info(" Starting land job processing...")

    # testing the data with manual file
    
    try:
        df = pd.read_parquet(r'ltd_2022-12-01.parquet')
        logging.info(f" Loaded parquet file with {len(df)} total records.")
    except Exception as e:
        logging.exception(" Failed to load parquet file.")
        return

    # Load the data from yesterday's UTC S3 folder
    # try:
    #     yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    #     year = yesterday.strftime('%Y')
    #     month = yesterday.strftime('%m')
    #     day = yesterday.strftime('%d')

    #     bucket_name = 'XXXX'
    #     s3_prefix = f'dailyexport/XXXX/year={year}/month={month}/day={day}/'
    #     file_name = f'ltd_{year}-{month}-{day}.parquet'
    #     s3_path = f's3://{bucket_name}/{s3_prefix}{file_name}'

    #     logging.info(f"Attempting to read parquet file from: {s3_path}")

    #     df = pd.read_parquet(
    #         s3_path,
    #         engine='pyarrow',
    #         storage_options={"anon": False}  # assumes AWS credentials are configured
    #     )
    #     logging.info(f"Loaded parquet file with {len(df)} total records.")
    # except Exception as e:
    #     logging.exception("Failed to load parquet file from S3 path. Either no data for this date: {yesterday} was avialable. or Exception {e} was found")
    #     return



    try:
        sample_data = list(df['asset_id'].unique())
        sd = sample_data[15]
        df_sample = df[df['asset_id'] == sd]
        logging.info(f" Selected asset ID: {sd} with {len(df_sample)} records.")
    except Exception as e:
        logging.exception(" Failed to extract sample asset data.")
        return

    try:
        polygons_df, clustered_points_df, original_df = generate_cluster_polygons_for_asset(df_sample, sd)
        logging.info(f"Generated {len(polygons_df)} polygons for asset {sd}.")
    except Exception as e:
        logging.exception(" Failed during polygon generation.")
        return

    try:
        polygons_df['area'] = polygons_df['polygon'].apply(calculate_polygon_area)
        logging.info(" Calculated area for each polygon.")
    except Exception as e:
        logging.exception(" Failed to calculate polygon areas.")
        return


    records = []
    try:
        for index, row in polygons_df.iterrows():
            polygon = row['polygon']
            distance = calculate_distance(polygon)
            duration = calculate_duration(polygon, original_df)
            area = row['area']

            record = {
                "source": 20,
                "state": 0,
                "event_type": 'dbs',
                "start_time": str(original_df['location_time'].iloc[0]),
                "end_time": str(original_df['location_time'].iloc[-1]),
                "start_latitude": str(original_df['latitude'].iloc[0]),
                "start_longitude": str(original_df['longitude'].iloc[0]),
                "end_latitude": str(original_df['latitude'].iloc[-1]),
                "end_longitude": str(original_df['longitude'].iloc[-1]),
                "polygon_geojson": create_geojson_polygons_only(pd.DataFrame([row])),
                "area": area,
                "distance": distance,
                "duration": duration,
                "asset_id": sd,
                "day": original_df['location_time'].iloc[0].strftime('%Y-%m-%d')
            }
            records.append(record)

        logging.info(f" Created {len(records)} final records after computing distance, duration, and packaging.")
    except Exception as e:
        logging.exception(" Failed while creating final records.")
        return

    try:
        final_df = pd.DataFrame(records)
        safe_timestamp = original_df['location_time'].iloc[0].strftime("%Y-%m-%d")
        final_df['polygon_geojson'] = final_df['polygon_geojson'].apply(convert_to_pure_json)
        final_df.to_parquet(f'Landjob_{safe_timestamp}.parquet', index=False)
        logging.info(f" Successfully saved {len(final_df)} records locally as Landjob_{safe_timestamp}.parquet.")
    except Exception as e:
        logging.exception(" Failed while saving local parquet file.")
        return

    # Save to S3
    # Use UTC time
    try:
        now = datetime.now(timezone.utc)
        safe_timestamp = now.strftime('%Y-%m-%d')
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        logging.info("successfully created safe_timestamp")

        # Construct the file and S3 path
        file_name = f'Landjob_{safe_timestamp}.parquet'
        bucket_name = 'XXXX'
        s3_directory = f'land_jobs/year={year}/month={month}/day={day}/'
        s3_path = f's3://{bucket_name}/{s3_directory}{file_name}'
        logging.info("successfully created s3 path")
    except Exception as e:
        logging.exception("Failed to create s3 path")
        

    # Save to S3
    try:
        final_df.to_parquet(
            s3_path,
            index=False,
            engine='pyarrow', 
            storage_options={"anon": False}  # assumes AWS credentials are configured
        )
        logging.info(f"Successfully uploaded file to S3 at {s3_path}.")
    except Exception as e:
        logging.exception("Failed to upload to S3.")



if __name__ == "__main__":
    main()
