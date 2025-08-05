# scripts/plugins/loaders/base.py

from abc import abstractmethod
from typing import Optional, Dict

from scripts.core.plugin_manager.interfaces import LoaderInterface
from scripts.core.data_container.container import DataContainer

class BaseLoader(LoaderInterface):
    """
    Abstract base class for all data Loader plugins.
    """
    @abstractmethod
    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> None:
        """
        Executes the loading logic, sending data to a target system.
        Subclasses must implement this method.
        """
        raise NotImplementedError