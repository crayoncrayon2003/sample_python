# scripts/plugins/validators/data_quality.py

import pandas as pd
from typing import Dict, Any, List, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DataQualityValidator:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "data_quality"

    def _validate_rule(self, df: pd.DataFrame, rule: Dict[str, Any]) -> List[str]:
        errors = []
        col_name, rule_type = rule.get("column"), rule.get("type")
        if not col_name or not rule_type: raise ValueError(f"Rule missing 'column' or 'type': {rule}")
        if col_name not in df.columns: raise KeyError(f"Column '{col_name}' in rule not in DataFrame.")
        series = df[col_name]
        if rule_type == "not_null":
            if series.isnull().any(): errors.append(f"Column '{col_name}' has nulls.")
        elif rule_type == "is_unique":
            if not series.is_unique: errors.append(f"Column '{col_name}' has duplicates.")
        elif rule_type == "in_range":
            min_val, max_val = rule.get('min'), rule.get('max')
            if not series[(series < min_val) | (series > max_val)].empty: errors.append(f"Column '{col_name}' has values out of range [{min_val}, {max_val}].")
        elif rule_type == "matches_regex":
            pattern = rule.get('pattern')
            if not pattern: raise ValueError("Rule 'matches_regex' needs 'pattern'.")
            if not series[~series.astype(str).str.match(pattern, na=False)].empty: errors.append(f"Column '{col_name}' has values not matching regex.")
        elif rule_type == "in_set":
            value_set = set(rule.get('values', []))
            if not value_set: raise ValueError("Rule 'in_set' needs 'values' list.")
            if not series[~series.isin(value_set)].empty: errors.append(f"Column '{col_name}' has values not in allowed set.")
        else: errors.append(f"Unknown rule type '{rule_type}'.")
        return errors

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        rules = params.get("rules", [])
        if not isinstance(rules, list): raise ValueError("'rules' parameter must be a list.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: DataQualityValidator received no DataFrame.")
            return data

        print("Performing data quality checks...")
        all_errors: List[str] = []
        for rule in rules:
            try:
                rule_errors = self._validate_rule(data.data, rule)
                if rule_errors: all_errors.extend(rule_errors)
            except (KeyError, ValueError) as e: raise e

        if all_errors:
            raise ValueError(f"Data quality validation failed:\n- " + "\n- ".join(all_errors))
        print("Data quality checks passed.")
        return data