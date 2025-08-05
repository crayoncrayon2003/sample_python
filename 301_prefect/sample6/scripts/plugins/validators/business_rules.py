# scripts/plugins/validators/business_rules.py

from typing import Dict, Any, List, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class BusinessRulesValidator:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "business_rules"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        rules = params.get("rules", [])
        if not isinstance(rules, list):
            raise ValueError("'rules' parameter must be a list of rule dictionaries.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: BusinessRulesValidator received no DataFrame.")
            return data

        df = data.data
        print("Validating custom business rules...")
        all_errors: List[str] = []
        for rule in rules:
            rule_name = rule.get('name')
            expression = rule.get('expression')
            if not rule_name or not expression:
                raise ValueError(f"Rule is missing 'name' or 'expression': {rule}")

            try:
                invalid_rows_df = df.query(expression)
                if not invalid_rows_df.empty:
                    error_msg = (f"Rule '{rule_name}' failed for {len(invalid_rows_df)} rows. "
                                 f"(Expression: '{expression}'). "
                                 f"Indices: {invalid_rows_df.index.tolist()[:5]}")
                    all_errors.append(error_msg)
            except Exception as e:
                raise ValueError(f"Error executing business rule '{rule_name}': {e}")

        if all_errors:
            error_summary = "\n- ".join(all_errors)
            raise ValueError(f"Business rule validation failed:\n- {error_summary}")

        print("All business rules passed.")
        return data