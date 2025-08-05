# scripts/plugins/validators/business_rules.py

import pandas as pd
from typing import Dict, Any, List, Optional

from .base import BaseValidator
from scripts.core.data_container.container import DataContainer

class BusinessRulesValidator(BaseValidator):
    """
    Validates the DataFrame against a set of custom business rules.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.rules = self.params.get("rules", [])
        if not isinstance(self.rules, list):
            raise ValueError("'rules' parameter must be a list of rule dictionaries.")

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("BusinessRulesValidator requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: BusinessRulesValidator received no DataFrame. Skipping.")
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
                invalid_rows_df = df.query(expression)
                if not invalid_rows_df.empty:
                    num_invalid = len(invalid_rows_df)
                    error_msg = (f"Rule '{rule_name}' failed for {num_invalid} rows. "
                                 f"(Expression: '{expression}'). "
                                 f"Indices: {invalid_rows_df.index.tolist()[:5]}")
                    all_errors.append(error_msg)
            except Exception as e:
                raise ValueError(f"Error executing business rule '{rule_name}': {e}")

        if all_errors:
            error_summary = "\n- ".join(all_errors)
            raise ValueError(f"Business rule validation failed:\n- {error_summary}")

        print("All business rules passed successfully.")
        return data