# scripts/plugins/validators/base.py

from abc import abstractmethod
from typing import Optional

from scripts.core.plugin_manager.interfaces import ValidatorInterface
from scripts.core.data_container.container import DataContainer

class BaseValidator(ValidatorInterface):
    """
    Abstract base class for all data Validator plugins.

    This class inherits from the `ValidatorInterface` and serves as a
    common parent for all concrete data validation implementations.
    It enforces that all subclasses implement the `execute` method.
    """

    @abstractmethod
    def execute(self, data: DataContainer) -> DataContainer:
        """
        Executes the validation logic on the input data.

        This method must be implemented by all concrete validator classes.
        It takes a DataContainer, performs a validation check on its
        contents, and should raise an exception if validation fails.
        If validation succeeds, it should return the original, unmodified
        DataContainer to be passed to the next step.

        Args:
            data (DataContainer): The input data to be validated.

        Returns:
            DataContainer: The original data container if validation is successful.
        
        Raises:
            Exception (or a more specific subclass): If validation fails.
        """
        # The implementation of this method is the core logic of each
        # specific validator plugin.
        raise NotImplementedError
