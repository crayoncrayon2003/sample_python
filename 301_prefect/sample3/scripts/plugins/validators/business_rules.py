# scripts/plugins/validators/business_rules.py

import pandas as pd
from typing import Dict, Any, List

from .base import BaseValidator
from scripts.core.data_container.container import DataContainer

class BusinessRulesValidator(BaseValidator):
    """
    Validates the DataFrame against a set of custom business rules.

    This validator uses pandas' `query` method or `eval` function to check for
    complex, inter-column relationships and other business-specific logic that
    goes beyond simple single-column checks.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the validator with a set of business rule expressions.

        Expected params:
            - rules (list of dict): A list of rule configurations. Each rule is a
              dictionary that must contain a 'name' and an 'expression'.
              The expression should be a valid pandas query string that
              returns the *invalid* rows.
              Example:
              [
                {
                  "name": "End time must be after start time",
                  "expression": "end_time <= start_time"
                },
                {
                  "name": "Discounted price must be lower than original price",
                  "expression": "discounted_price >= original_price"
                },
                {
                   "name": "Sum of percentages must be 100",
                   "expression": "abs(percent_a + percent_b - 100) > 0.001"
                }
              ]
        """
        super().__init__(params)
        self.rules = self.params.get("rules", [])
        if not isinstance(self.rules, list):
            raise ValueError("'rules' parameter must be a list of rule dictionaries.")

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Applies each business rule expression to the DataFrame.

        Args:
            data (DataContainer): The input container with the DataFrame to validate.

        Returns:
            DataContainer: The original container, if all rules pass.
        
        Raises:
            ValueError: If any of the business rules are violated.
        """
        if data.data is None:
            print("Warning: BusinessRulesValidator received a DataContainer with no DataFrame. Skipping.")
            return data
        
        df = data.data
        print("Validating custom business rules...")
        all_errors: List[str] = []

        for rule in self.rules:
            rule_name = rule.get('name')
            expression = rule.get('expression')

            if not rule_name or not expression:
                raise ValueError(f"Rule is missing 'name' or 'expression': {rule}")

            try:
                # Use df.query() to find rows that violate the rule.
                # The expression should be written to select the *invalid* rows.
                invalid_rows_df = df.query(expression)
                
                if not invalid_rows_df.empty:
                    num_invalid = len(invalid_rows_df)
                    error_msg = (
                        f"Rule '{rule_name}' failed for {num_invalid} rows. "
                        f"(Expression for invalid rows: '{expression}'). "
                        f"Example invalid row indices: {invalid_rows_df.index.tolist()[:5]}"
                    )
                    all_errors.append(error_msg)

            except Exception as e:
                # This can catch errors in the expression itself, like syntax errors
                # or references to non-existent columns.
                raise ValueError(f"Error executing business rule '{rule_name}': {e}")

        if all_errors:
            error_summary = "\n- ".join(all_errors)
            raise ValueError(f"Business rule validation failed:\n- {error_summary}")

        print("All business rules passed successfully.")
        return data