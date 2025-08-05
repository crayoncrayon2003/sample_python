# scripts/plugins/validators/data_quality.py

import pandas as pd
from typing import Dict, Any, List

from .base import BaseValidator
from scripts.core.data_container.container import DataContainer

class DataQualityValidator(BaseValidator):
    """
    Performs data quality checks on a DataFrame based on a set of rules.

    This validator applies user-defined rules to specific columns of a DataFrame,
    such as checking for nulls, uniqueness, value ranges, or regex patterns.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the validator with a set of data quality rules.

        Expected params:
            - rules (list of dict): A list of rule configurations. Each rule is a
              dictionary that must contain 'column' and 'type' keys.
              Example:
              [
                { "column": "id", "type": "not_null" },
                { "column": "id", "type": "is_unique" },
                { "column": "age", "type": "in_range", "min": 0, "max": 120 },
                { "column": "email", "type": "matches_regex", "pattern": "^\\S+@\\S+\\.\\S+$" },
                { "column": "status", "type": "in_set", "values": ["active", "inactive"] }
              ]
        """
        super().__init__(params)
        self.rules = self.params.get("rules", [])
        if not isinstance(self.rules, list):
            raise ValueError("'rules' parameter must be a list of rule dictionaries.")

    def _validate_rule(self, df: pd.DataFrame, rule: Dict[str, Any]) -> List[str]:
        """Validates a single rule against the DataFrame and returns a list of error messages."""
        errors = []
        col_name = rule.get("column")
        rule_type = rule.get("type")

        if not col_name or not rule_type:
            raise ValueError(f"Rule is missing 'column' or 'type': {rule}")
        if col_name not in df.columns:
            raise KeyError(f"Column '{col_name}' specified in a rule was not found in the DataFrame.")

        series = df[col_name]
        
        # --- Rule Implementations ---
        if rule_type == "not_null":
            if series.isnull().any():
                null_rows = series[series.isnull()].index.tolist()
                errors.append(f"Column '{col_name}' has null values at rows: {null_rows[:5]}")
        
        elif rule_type == "is_unique":
            if not series.is_unique:
                duplicates = series[series.duplicated()].unique().tolist()
                errors.append(f"Column '{col_name}' has duplicate values: {duplicates[:5]}")
        
        elif rule_type == "in_range":
            min_val, max_val = rule.get('min'), rule.get('max')
            out_of_range = series[(series < min_val) | (series > max_val)]
            if not out_of_range.empty:
                errors.append(f"Column '{col_name}' has values out of range [{min_val}, {max_val}]: {out_of_range.unique().tolist()[:5]}")

        elif rule_type == "matches_regex":
            pattern = rule.get('pattern')
            if not pattern: raise ValueError("Rule 'matches_regex' requires a 'pattern'.")
            non_matching = series[~series.astype(str).str.match(pattern, na=False)]
            if not non_matching.empty:
                errors.append(f"Column '{col_name}' has values not matching regex '{pattern}': {non_matching.unique().tolist()[:5]}")

        elif rule_type == "in_set":
            value_set = set(rule.get('values', []))
            if not value_set: raise ValueError("Rule 'in_set' requires a 'values' list.")
            out_of_set = series[~series.isin(value_set)]
            if not out_of_set.empty:
                 errors.append(f"Column '{col_name}' has values not in the allowed set {value_set}: {out_of_set.unique().tolist()[:5]}")
        
        else:
            errors.append(f"Unknown rule type '{rule_type}' for column '{col_name}'.")
            
        return errors


    def execute(self, data: DataContainer) -> DataContainer:
        """
        Applies all configured data quality rules to the DataFrame.

        Args:
            data (DataContainer): The input container with the DataFrame to validate.

        Returns:
            DataContainer: The original container, if validation is successful.
        
        Raises:
            ValueError: If any of the data quality rules are violated.
        """
        if data.data is None:
            print("Warning: DataQualityValidator received a DataContainer with no DataFrame. Skipping.")
            return data
        
        print("Performing data quality checks...")
        all_errors: List[str] = []

        for rule in self.rules:
            try:
                rule_errors = self._validate_rule(data.data, rule)
                if rule_errors:
                    all_errors.extend(rule_errors)
            except (KeyError, ValueError) as e:
                # Catch configuration errors and propagate them
                raise e

        if all_errors:
            error_summary = "\n- ".join(all_errors)
            raise ValueError(f"Data quality validation failed with {len(all_errors)} errors:\n- {error_summary}")

        print("Data quality checks passed successfully.")
        return data