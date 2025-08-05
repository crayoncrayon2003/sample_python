# scripts/plugins/transformers/shapefile_processor.py

from pathlib import Path
from typing import Dict, Any, List, Optional
import geopandas as gpd
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ShapefileProcessor:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "shapefile_processor"

    def _find_shapefile(self, file_paths: List[Path]) -> Path | None:
        for path in file_paths:
            if path.suffix.lower() == '.shp': return path
        return None

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        target_crs = params.get("target_crs")
        gpd_options = params.get("geopandas_options", {})
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if not data.file_paths:
            print("Warning: ShapefileProcessor received no file paths.")
            return data

        shapefile_path = self._find_shapefile(data.file_paths)
        if not shapefile_path: raise FileNotFoundError("No .shp file found in file paths.")

        print(f"Processing shapefile: {shapefile_path}")
        try:
            gdf = gpd.read_file(shapefile_path, **gpd_options)
            print(f"Loaded {len(gdf)} features. Original CRS: {gdf.crs}")
            if target_crs and gdf.crs and gdf.crs != target_crs:
                print(f"Reprojecting to {target_crs}...")
                gdf = gdf.to_crs(target_crs)
                print(f"New CRS: {gdf.crs}")
        except Exception as e:
            print(f"ERROR processing shapefile {shapefile_path}: {e}")
            raise

        output_container = DataContainer(data=gdf)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['shapefile_processed'] = {'source_file': str(shapefile_path), 'feature_count': len(gdf)}
        return output_container