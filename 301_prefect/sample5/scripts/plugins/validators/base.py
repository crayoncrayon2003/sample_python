# scripts/plugins/validators/base.py

from abc import abstractmethod
from typing import Optional, Dict

from scripts.core.plugin_manager.interfaces import ValidatorInterface
from scripts.core.data_container.container import DataContainer

class BaseValidator(ValidatorInterface):
    """
    Abstract base class for all data Validator plugins.
    """
    @abstractmethod
    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        """
        Executes the validation logic on the input data.
        Subclasses must implement this method. It should raise an exception
        on validation failure and return the original data container on success.
        """
        raise NotImplementedError