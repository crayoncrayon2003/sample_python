# scripts/plugins/transformers/base.py

from abc import abstractmethod
from typing import Optional

from scripts.core.plugin_manager.interfaces import TransformerInterface
from scripts.core.data_container.container import DataContainer

class BaseTransformer(TransformerInterface):
    """
    Abstract base class for all data Transformer plugins.

    This class inherits from the `TransformerInterface` and serves as a
    common parent for all concrete data transformation implementations.
    It enforces that all subclasses implement the `execute` method.
    """

    @abstractmethod
    def execute(self, data: DataContainer) -> DataContainer:
        """
        Executes the transformation logic on the input data.

        This method must be implemented by all concrete transformer classes.
        It takes a DataContainer, performs a transformation on its
        contents (typically the DataFrame), and returns a DataContainer
        with the transformed data.

        Args:
            data (DataContainer): The input data to be transformed.

        Returns:
            DataContainer: The transformed data.
        """
        # The implementation of this method is the core logic of each
        # specific transformer plugin.
        raise NotImplementedError