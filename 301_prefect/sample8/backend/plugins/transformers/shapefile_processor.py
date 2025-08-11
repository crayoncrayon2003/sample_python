# backend/plugins/transformers/shapefile_processor.py

from pathlib import Path
from typing import Dict, Any, Optional
import geopandas as gpd
import pandas as pd
import pluggy

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ShapefileProcessor:
    """
    (File-based) Processes a shapefile and saves it as a Parquet file with WKT geometry.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "shapefile_processor"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "title": "Input Shapefile Path (.shp)",
                    "description": "Path to the main .shp file of the shapefile."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output Parquet Path",
                    "description": "Path to save the data as a Parquet file, with geometry converted to WKT."
                },
                "target_crs": {
                    "type": "string",
                    "title": "Target CRS (Optional)",
                    "description": "The EPSG code to reproject the data into (e.g., 'EPSG:4326')."
                }
            },
            "required": ["input_path", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        target_crs = params.get("target_crs")

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input shapefile not found at: {input_path}")

        print(f"Processing shapefile: {input_path}")
        try:
            gdf = gpd.read_file(input_path)
            if target_crs and gdf.crs and gdf.crs != target_crs:
                gdf = gdf.to_crs(target_crs)

            # Convert geometry to WKT string to be compatible with Parquet
            df = pd.DataFrame(gdf.drop(columns='geometry'))
            df['geometry_wkt'] = gdf.geometry.to_wkt()

            print(f"Processed {len(df)} features. Saving to '{output_path}'.")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(output_path, index=False)

        except Exception as e:
            print(f"ERROR processing shapefile {input_path}: {e}")
            raise

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container