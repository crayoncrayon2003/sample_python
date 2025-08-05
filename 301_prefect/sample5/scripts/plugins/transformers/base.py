# scripts/plugins/transformers/base.py

from abc import abstractmethod
from typing import Optional, Dict

from scripts.core.plugin_manager.interfaces import TransformerInterface
from scripts.core.data_container.container import DataContainer

class BaseTransformer(TransformerInterface):
    """
    Abstract base class for all data Transformer plugins.
    """
    @abstractmethod
    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        """
        Executes the transformation logic on the input data.
        Subclasses must implement this method. It is expected to receive
        its primary input via inputs['input_data'].
        """
        raise NotImplementedError