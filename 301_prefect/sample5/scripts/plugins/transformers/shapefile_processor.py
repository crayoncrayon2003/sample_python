# scripts/plugins/transformers/shapefile_processor.py

from pathlib import Path
from typing import Dict, Any, List, Optional
import geopandas as gpd

from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class ShapefileProcessor(BaseTransformer):
    """
    Processes a shapefile (.shp) and loads it into a GeoDataFrame.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.target_crs = self.params.get("target_crs")
        self.gpd_options = self.params.get("geopandas_options", {})

    def _find_shapefile(self, file_paths: List[Path]) -> Path | None:
        for path in file_paths:
            if path.suffix.lower() == '.shp': return path
        return None

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("ShapefileProcessor requires a single input named 'input_data'.")
        data = inputs['input_data']

        if not data.file_paths:
            print("Warning: ShapefileProcessor received a DataContainer with no file paths. Skipping.")
            return data

        shapefile_path = self._find_shapefile(data.file_paths)
        if not shapefile_path:
            raise FileNotFoundError("No .shp file found in the provided file paths for the ShapefileProcessor.")

        print(f"Processing shapefile: {shapefile_path}")

        try:
            gdf = gpd.read_file(shapefile_path, **self.gpd_options)
            print(f"Successfully loaded shapefile into a GeoDataFrame with {len(gdf)} features.")
            print(f"Original CRS: {gdf.crs}")

            if self.target_crs and gdf.crs and gdf.crs != self.target_crs:
                print(f"Reprojecting GeoDataFrame to {self.target_crs}...")
                gdf = gdf.to_crs(self.target_crs)
                print(f"New CRS: {gdf.crs}")
        except Exception as e:
            print(f"ERROR processing shapefile {shapefile_path}: {e}")
            raise

        output_container = DataContainer(data=gdf)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['shapefile_processed'] = {
            'source_file': str(shapefile_path),
            'original_crs': str(gdf.crs),
            'feature_count': len(gdf)
        }

        return output_container