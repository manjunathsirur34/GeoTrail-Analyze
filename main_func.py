import geopy.distance
from shapely.geometry import Polygon
import json

def convert_to_pure_json(geojson_str):
    if isinstance(geojson_str, (str, bytes, bytearray)):
        return geojson_str  
    try:
        geojson_dict = json.loads(geojson_str[0])  
        return json.dumps(geojson_dict) 
    except (json.JSONDecodeError, TypeError, IndexError):
        return None  

def create_geojson_polygons_only(polygons_df):
    geojson_polygons = []
    
    for _, row in polygons_df.iterrows():
        polygon_coordinates = [list(coord) for coord in row['polygon']]
        
        # Ensure the polygon is closed
        if polygon_coordinates[0] != polygon_coordinates[-1]:
            polygon_coordinates.append(polygon_coordinates[0])
        
        # Flip coordinates to [lon, lat] format
        formatted_coords = [[lon, lat] for lat, lon in polygon_coordinates]

        geojson_geometry = {
            "type": "Polygon",
            "coordinates": [formatted_coords]
        }

        geojson_polygons.append(json.dumps(geojson_geometry))

    return geojson_polygons


def calculate_polygon_area(polygon):
    """Calculates the area of a polygon in hectares."""
    polygon_shape = Polygon(polygon)
    area_sq_meters = polygon_shape.area
    area_hectares = area_sq_meters / 10000  
    return area_hectares

def calculate_distance(polygon):
  """Calculates the total distance of a polygon."""
  total_distance = 0
  for i in range(len(polygon) - 1):
    coords_1 = (polygon[i][1], polygon[i][0])
    coords_2 = (polygon[i + 1][1], polygon[i + 1][0])
    total_distance += geopy.distance.geodesic(coords_1, coords_2).m
  coords_1 = (polygon[-1][1], polygon[-1][0])
  coords_2 = (polygon[0][1], polygon[0][0])
  total_distance += geopy.distance.geodesic(coords_1, coords_2).m
  return total_distance


def calculate_duration(polygon, original_df):
    """Calculates the duration of a polygon."""
    return (original_df['location_time'].iloc[-1] - original_df['location_time'].iloc[0]).total_seconds()
