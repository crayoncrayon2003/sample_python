# scripts/core/plugin_manager/interfaces.py

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from ..data_container.container import DataContainer


class PluginInterface(ABC):
    """
    The abstract base class that all plugins must inherit from.
    """
    def __init__(self, params: Dict[str, Any]):
        self.params = params

    @abstractmethod
    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> Optional[DataContainer]:
        """
        The main execution method for the plugin.

        Args:
            inputs (Dict[str, Optional[DataContainer]]): A dictionary of named inputs
                from upstream steps. The keys correspond to the keys specified in
                the `upstream_nodes` of the builder. For single-input steps,
                this dictionary will typically contain one item, e.g., {'input_data': ...}.

        Returns:
            Optional[DataContainer]: The resulting data container to be passed
                to downstream steps. It can be None for Loader plugins.
        """
        raise NotImplementedError("Each plugin must implement the 'execute' method.")


class ExtractorInterface(PluginInterface):
    """
    Interface for plugins that extract data and start a pipeline.
    Extractors have no upstream dependencies, so their `inputs` dict will be empty.
    """
    @abstractmethod
    def execute(self, inputs: Dict[str, Optional[DataContainer]] = None) -> DataContainer:
        # The signature is updated, but the logic remains: it expects no actual input.
        pass


class CleanserInterface(PluginInterface):
    """ Marker interface for plugins that perform data cleansing. """
    pass

class TransformerInterface(PluginInterface):
    """ Marker interface for plugins that transform data. """
    pass

class ValidatorInterface(PluginInterface):
    """ Marker interface for plugins that validate data. """
    pass


class LoaderInterface(PluginInterface):
    """
    Interface for plugins that load data into a target system (a sink).
    """
    @abstractmethod
    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> None:
        # The signature is updated, and it still returns nothing.
        pass