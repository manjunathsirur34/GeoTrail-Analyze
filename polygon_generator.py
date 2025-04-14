from sklearn.cluster import DBSCAN
from scipy.spatial import ConvexHull
import numpy as np
import pandas as pd
from math import atan2, degrees
import warnings
warnings.filterwarnings("ignore")

def calculate_angle(lat1, lon1, lat2, lon2, lat3, lon3, thresh=180):
    lat1, lon1 = np.radians(lat1), np.radians(lon1)
    lat2, lon2 = np.radians(lat2), np.radians(lon2)
    lat3, lon3 = np.radians(lat3), np.radians(lon3)

    y1 = np.sin(lon2 - lon1) * np.cos(lat2)
    x1 = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(lon2 - lon1)
    bearing1 = atan2(y1, x1)

    y2 = np.sin(lon3 - lon2) * np.cos(lat3)
    x2 = np.cos(lat2) * np.sin(lat3) - np.sin(lat2) * np.cos(lat3) * np.cos(lon3 - lon2)
    bearing2 = atan2(y2, x2)

    angle = degrees(bearing2 - bearing1)

    if angle > thresh:
        angle -= 360
    elif angle <= -thresh:
        angle += 360

    return angle

def generate_nearby_points(lat, lon, num_points=500, radius_meters=5):
    lat_points = []
    lon_points = []
    for _ in range(num_points):
        u = np.random.uniform(0, 1)
        v = np.random.uniform(0, 1)
        w = radius_meters * np.sqrt(u)
        t = 2 * np.pi * v
        x = w * np.cos(t)
        y = w * np.sin(t)
        dx = x / (111132 * np.cos(np.radians(lat)))
        dy = y / 111132
        lat_points.append(lat + dy)
        lon_points.append(lon + dx)
    return lat_points, lon_points

def generate_cluster_polygons_for_asset(df_raw,
                                        asset_id,
                                        lower_threshold=150, 
                                        upper_threshold=180, 
                                        eps=0.001, 
                                        min_samples=2500):
  df = df_raw.copy()
  df = df.set_index('location_time').resample('2min').last().reset_index()
  df['angle'] = np.nan
  for i in range(1, len(df) - 1):
      df.at[i, 'angle'] = calculate_angle(df['latitude'].iloc[i-1], df['longitude'].iloc[i-1],
                                          df['latitude'].iloc[i], df['longitude'].iloc[i],
                                          df['latitude'].iloc[i+1], df['longitude'].iloc[i+1])

  filtered_df = df[((df['angle'] < -lower_threshold) & (df['angle'] > -upper_threshold)) |
                    ((df['angle'] > lower_threshold) & (df['angle'] < upper_threshold))]

  all_latitudes_new = []
  all_longitudes_new = []

  for _, row in filtered_df.iterrows():
      lat_points, lon_points = generate_nearby_points(row['latitude'], row['longitude'])
      all_latitudes_new.extend(lat_points)
      all_longitudes_new.extend(lon_points)

  combined_data = np.column_stack((all_latitudes_new, all_longitudes_new))
  dbscan = DBSCAN(eps=eps, min_samples=min_samples)
  clusters = dbscan.fit_predict(combined_data)

  combined_df = pd.DataFrame({'latitude': combined_data[:, 0], 'longitude': combined_data[:, 1], 'cluster': clusters})

  polygon_list = []
  for cluster_label in np.unique(clusters):
      if cluster_label == -1:
          continue
      cluster_points = combined_df[combined_df['cluster'] == cluster_label][['longitude', 'latitude']].values
      if len(cluster_points) >= 3:
          hull = ConvexHull(cluster_points)
          polygon = [(cluster_points[vertex][0], cluster_points[vertex][1]) for vertex in hull.vertices]
          polygon_list.append({'asset_id': asset_id, 'cluster': cluster_label, 'polygon': polygon})

  polygons_df = pd.DataFrame(polygon_list)
  return polygons_df, combined_df, df  