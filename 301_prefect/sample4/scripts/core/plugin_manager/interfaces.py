# scripts/core/plugin_manager/interfaces.py

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

# .. は親ディレクトリを指す相対インポート。
# coreパッケージ内の別サブパッケージからDataContainerをインポートする。
from ..data_container.container import DataContainer


class PluginInterface(ABC):
    """
    The abstract base class that all plugins must inherit from.

    It defines the basic contract for a plugin: it must be initializable
    with a dictionary of parameters and must have an `execute` method.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the plugin with parameters from the config file.

        Args:
            params (Dict[str, Any]): A dictionary of parameters defined
                                     under the 'params' key in a pipeline step.
        """
        self.params = params

    @abstractmethod
    def execute(self, data: Optional[DataContainer] = None) -> Optional[DataContainer]:
        """
        The main execution method for the plugin.
        This method must be implemented by all concrete plugin classes.

        Args:
            data (Optional[DataContainer]): The data container from the
                previous step. It's None for Extractor plugins.

        Returns:
            Optional[DataContainer]: The resulting data container to be passed
                to the next step. It can be None for Loader plugins.
        """
        raise NotImplementedError("Each plugin must implement the 'execute' method.")


class ExtractorInterface(PluginInterface):
    """
    Interface for plugins that extract data and start a pipeline.

    The `execute` method for an Extractor should not expect an input
    DataContainer and must return a new DataContainer.
    """
    @abstractmethod
    def execute(self) -> DataContainer:
        # This overrides the base class signature to be more specific.
        # It takes no input data and must return a DataContainer.
        pass


class CleanserInterface(PluginInterface):
    """
    Marker interface for plugins that perform data cleansing.
    
    The `execute` method should expect a DataContainer and return one.
    """
    pass


class TransformerInterface(PluginInterface):
    """
    Marker interface for plugins that transform data.

    The `execute` method should expect a DataContainer and return one.
    """
    pass


class ValidatorInterface(PluginInterface):
    """
    Marker interface for plugins that validate data.
    
    The `execute` method should expect a DataContainer and return one.
    It might return the input container unmodified if validation passes.
    """
    pass


class LoaderInterface(PluginInterface):
    """
    Interface for plugins that load data into a target system (a sink).

    The `execute` method for a Loader expects an input DataContainer
    but is not expected to return anything to the pipeline.
    """
    @abstractmethod
    def execute(self, data: DataContainer) -> None:
        # This overrides the base class signature.
        # It takes input data but returns nothing.
        pass