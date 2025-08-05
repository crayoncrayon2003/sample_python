# scripts/plugins/loaders/base.py

from abc import abstractmethod
from typing import Optional

from scripts.core.plugin_manager.interfaces import LoaderInterface
from scripts.core.data_container.container import DataContainer

class BaseLoader(LoaderInterface):
    """
    Abstract base class for all data Loader plugins.

    This class inherits from the `LoaderInterface` and serves as a
    common parent for all concrete data loading implementations.
    It enforces that all subclasses implement the `execute` method,
    which is expected to perform a load operation and return nothing.
    """

    @abstractmethod
    def execute(self, data: DataContainer) -> None:
        """
        Executes the loading logic, sending data to a target system.

        This method must be implemented by all concrete loader classes.
        It takes a DataContainer, performs the load operation (e.g., writing
        to a file, posting to an API), and does not return anything as it
        is considered a terminal step in the pipeline.

        Args:
            data (DataContainer): The final, processed data to be loaded.
        """
        # The implementation of this method is the core logic of each
        # specific loader plugin.
        raise NotImplementedError