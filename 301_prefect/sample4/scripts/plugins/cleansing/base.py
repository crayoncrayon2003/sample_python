# scripts/plugins/cleansing/base.py

from abc import abstractmethod
from typing import Optional

from scripts.core.plugin_manager.interfaces import CleanserInterface
from scripts.core.data_container.container import DataContainer

class BaseCleanser(CleanserInterface):
    """
    Abstract base class for all data Cleanser plugins.

    This class inherits from the `CleanserInterface` and serves as a
    common parent for all concrete data cleansing and preparation implementations.
    It enforces that all subclasses implement the `execute` method.
    """

    @abstractmethod
    def execute(self, data: DataContainer) -> DataContainer:
        """
        Executes the cleansing logic on the input data.

        This method must be implemented by all concrete cleanser classes.
        It takes a DataContainer, performs a cleansing operation on its
        contents, and returns a (potentially modified) DataContainer.

        Args:
            data (DataContainer): The input data to be cleansed.

        Returns:
            DataContainer: The cleansed data.
        """
        # The implementation of this method is the core logic of each
        # specific cleanser plugin.
        raise NotImplementedError