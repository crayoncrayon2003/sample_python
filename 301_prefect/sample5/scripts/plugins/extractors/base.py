# scripts/plugins/extractors/base.py

from abc import abstractmethod
from typing import Dict, Optional

from scripts.core.plugin_manager.interfaces import ExtractorInterface
from scripts.core.data_container.container import DataContainer

class BaseExtractor(ExtractorInterface):
    """
    Abstract base class for all Extractor plugins.

    This class inherits from the `ExtractorInterface` and serves as a
    common parent for all concrete extractor implementations. It can be
    used to add shared functionality or properties that are relevant
    to all extractors in the future.

    All subclasses must implement the `execute` method.
    """

    @abstractmethod
    def execute(self, inputs: Dict[str, Optional[DataContainer]] = None) -> DataContainer:
        """
        Extracts data from a source and returns it in a DataContainer.

        This method must be implemented by all concrete extractor classes.
        It should not expect any input data and is responsible for creating
        the initial DataContainer for the pipeline.
        The `inputs` dictionary is expected to be empty.

        Args:
            inputs (Dict[str, Optional[DataContainer]], optional): Inputs from
                upstream nodes. Should be empty for an extractor. Defaults to None.

        Returns:
            DataContainer: A new container holding the extracted data.
        """
        if inputs:
            print(f"Warning: Extractor plugin received unexpected inputs: {list(inputs.keys())}")

        raise NotImplementedError