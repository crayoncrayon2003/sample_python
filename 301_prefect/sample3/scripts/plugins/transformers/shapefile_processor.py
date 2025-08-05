# scripts/plugins/transformers/shapefile_processor.py

from pathlib import Path
from typing import Dict, Any
import geopandas as gpd

from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class ShapefileProcessor(BaseTransformer):
    """
    Processes a shapefile (.shp) and loads it into a GeoDataFrame.

    This transformer identifies a .shp file from the input file paths and uses
    the GeoPandas library to read it. A shapefile is typically a collection
    of files (.shp, .shx, .dbf, etc.); this processor expects them to be
    in the same directory. The result is a GeoDataFrame, which is a pandas
    DataFrame with a special 'geometry' column.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the Shapefile processor.

        Expected params:
            - target_crs (str, optional): The EPSG code (e.g., 'EPSG:4326') to
              reproject the data into. If not provided, the original CRS
              (Coordinate Reference System) is kept.
            - geopandas_options (dict, optional): A dictionary of options to
              pass to the `gpd.read_file` function.
        """
        super().__init__(params)
        self.target_crs = self.params.get("target_crs")
        self.gpd_options = self.params.get("geopandas_options", {})

    def _find_shapefile(self, file_paths: List[Path]) -> Path | None:
        """Finds the first .shp file in a list of paths."""
        for path in file_paths:
            if path.suffix.lower() == '.shp':
                return path
        return None

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Reads a shapefile and creates a GeoDataFrame.

        Args:
            data (DataContainer): The input container, expected to have paths
                                  to the extracted shapefile components.

        Returns:
            DataContainer: A new container with the data loaded into a GeoDataFrame.
        """
        if not data.file_paths:
            print("Warning: ShapefileProcessor received a DataContainer with no file paths. Skipping.")
            return data

        shapefile_path = self._find_shapefile(data.file_paths)

        if not shapefile_path:
            raise FileNotFoundError("No .shp file found in the provided file paths for the ShapefileProcessor.")

        print(f"Processing shapefile: {shapefile_path}")

        try:
            # GeoPandas reads the .shp file and automatically finds related files
            gdf = gpd.read_file(shapefile_path, **self.gpd_options)
            print(f"Successfully loaded shapefile into a GeoDataFrame with {len(gdf)} features.")
            print(f"Original CRS: {gdf.crs}")

            # Reproject to the target CRS if specified
            if self.target_crs and gdf.crs and gdf.crs != self.target_crs:
                print(f"Reprojecting GeoDataFrame to {self.target_crs}...")
                gdf = gdf.to_crs(self.target_crs)
                print(f"New CRS: {gdf.crs}")

        except Exception as e:
            print(f"ERROR processing shapefile {shapefile_path}: {e}")
            raise

        # Create a new DataContainer with the GeoDataFrame
        # Note: The 'data' attribute now holds a GeoDataFrame, which is a
        # subclass of pandas.DataFrame.
        output_container = DataContainer(data=gdf)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['shapefile_processed'] = {
            'source_file': str(shapefile_path),
            'original_crs': str(gdf.crs),
            'feature_count': len(gdf)
        }

        return output_container