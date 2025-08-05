# scripts/core/config/validator.py

from typing import Dict, Any, Type, Optional
from pydantic import BaseModel, ValidationError

class ConfigValidator:
    """
    Validates configuration dictionaries against a Pydantic model.

    This class provides a simple interface to check if a loaded configuration
    (e.g., from a YAML file) conforms to a predefined schema, ensuring that
    all required fields are present and have the correct data types.
    """

    def __init__(self, schema_model: Type[BaseModel]):
        """
        Initializes the validator with a Pydantic model as the schema.

        Args:
            schema_model (Type[BaseModel]): The Pydantic model class that
                defines the expected structure and types of the configuration.
        """
        if not issubclass(schema_model, BaseModel):
            raise TypeError("schema_model must be a Pydantic BaseModel subclass.")
        self.schema_model = schema_model

    def validate(self, config_data: Dict[str, Any]) -> Optional[BaseModel]:
        """
        Validates the given configuration data against the schema.

        Args:
            config_data (Dict[str, Any]): The configuration dictionary to validate.

        Returns:
            Optional[BaseModel]: A Pydantic model instance populated with the
                validated and type-coerced data if validation is successful.
                Returns None if validation fails.
        """
        try:
            # Pydantic will automatically check for required fields,
            # validate data types, and perform type coercion.
            validated_config = self.schema_model.model_validate(config_data)
            print("Configuration validation successful.")
            return validated_config
        except ValidationError as e:
            # The error message from Pydantic is very detailed and user-friendly.
            print(f"Configuration validation failed:\n{e}")
            return None

# --- Example Usage ---
# You would define your expected configuration schema somewhere, for example:
#
# from pydantic import BaseModel, Field
# from typing import Optional
#
# class DatabaseConfig(BaseModel):
#     host: str = "localhost"
#     port: int = 5432
#     user: str
#     password: str = Field(..., repr=False) # repr=False hides it in logs
#
# class FtpConfig(BaseModel):
#     host: str
#     user: str
#     password: str = Field(..., repr=False)
#
# class FrameworkConfigSchema(BaseModel):
#     database: Optional[DatabaseConfig] = None
#     ftp_server: Optional[FtpConfig] = None
#
#
# if __name__ == '__main__':
#     # This demonstrates how the validator would be used.
#
#     # 1. Define the validator with the schema
#     validator = ConfigValidator(schema_model=FrameworkConfigSchema)
#
#     # 2. Provide some configuration data to validate
#     good_config = {
#         "database": {
#             "user": "admin",
#             "password": "secure_password_123"
#         }
#     }
#     
#     bad_config = {
#         "ftp_server": {
#             "host": "ftp.example.com",
#             # 'user' is missing, which is a required field.
#         }
#     }
#
#     # 3. Run validation
#     print("--- Validating good config ---")
#     validated = validator.validate(good_config)
#     if validated:
#         print(f"Accessing validated data: {validated.database.host}")
#
#     print("\n--- Validating bad config ---")
#     validator.validate(bad_config)
#