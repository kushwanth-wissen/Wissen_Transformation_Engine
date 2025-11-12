import yaml
from typing import Dict, Any

class ConfigValidationError(Exception):
    """Custom exception for configuration validation errors."""
    pass

class ConfigValidator:
    @staticmethod
    def validate_source_config(config: Dict[str, Any]) -> None:
        """
        Validates the source configuration structure.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary from YAML
        
        Raises:
            ConfigValidationError: If configuration is invalid
        """
        # Check required top-level keys
        required_keys = ["project", "sources", "data_transformations", "destination"]
        for key in required_keys:
            if key not in config:
                raise ConfigValidationError(f"Missing required key: {key}")
        
        # Validate sources
        for idx, source in enumerate(config["sources"]):
            if "type" not in source:
                raise ConfigValidationError(f"Source {idx} missing 'type' field")
            if "enabled" not in source:
                raise ConfigValidationError(f"Source {idx} missing 'enabled' field")
            
            # Validate source-specific requirements
            if source["type"] == "csv":
                if "path_list" not in source:
                    raise ConfigValidationError("CSV source missing 'path_list'")
            elif source["type"] in ["mysql", "postgres"]:
                # Accept either a credentials_key (legacy) or a credentials_env pointing to an env file
                # Do NOT require 'query' here because queries can be provided via CLI at runtime.
                if ("credentials_key" not in source and "credentials_env" not in source):
                    raise ConfigValidationError(f"{source['type']} source missing 'credentials_key' or 'credentials_env'")
            elif source["type"] == "mongodb":
                # Accept credentials via credentials_key (legacy) or credentials_env.
                # Collection can be provided in the config but may also come from CLI.
                if ("credentials_key" not in source and "credentials_env" not in source):
                    raise ConfigValidationError("MongoDB source missing 'credentials_key' or 'credentials_env'")
        
        # Validate destination
        required_dest_keys = ["type", "format", "mode"]
        dest = config.get("destination")
        if isinstance(dest, dict):
            for key in required_dest_keys + ["base_path"]:
                if key not in dest:
                    raise ConfigValidationError(f"Destination missing required key: {key}")
        elif isinstance(dest, list):
            # Validate each destination entry
            for idx, d in enumerate(dest):
                if not isinstance(d, dict):
                    raise ConfigValidationError(f"Destination entry {idx} is not a mapping")
                for key in required_dest_keys:
                    if key not in d:
                        # base_path may be optional for some cloud destinations; only require type/format/mode
                        raise ConfigValidationError(f"Destination entry {idx} missing required key: {key}")
        else:
            raise ConfigValidationError("Destination configuration must be a mapping or a list of mappings")

    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """
        Loads and validates the YAML configuration file.
        
        Args:
            config_path (str): Path to the YAML configuration file
        
        Returns:
            Dict[str, Any]: Validated configuration dictionary
        
        Raises:
            ConfigValidationError: If configuration file is invalid or missing
        """
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                
            # Validate the loaded configuration
            ConfigValidator.validate_source_config(config)
            return config
            
        except FileNotFoundError:
            raise ConfigValidationError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise ConfigValidationError(f"Invalid YAML format: {str(e)}")
        except Exception as e:
            raise ConfigValidationError(f"Error loading configuration: {str(e)}")

    @staticmethod
    def validate_cli_args(source_type: str, input_path: str, destination: str) -> None:
        """
        Validates command-line arguments.
        
        Args:
            source_type (str): Type of the source (csv, mysql, mongodb, etc.)
            input_path (str): Input path or query
            destination (str): Destination type (local, azure, s3)
        
        Raises:
            ConfigValidationError: If arguments are invalid
        """
        valid_sources = ["csv", "mysql", "postgres", "mongodb", "json", "api"]
        valid_destinations = ["local", "azure", "s3"]

        if source_type not in valid_sources:
            raise ConfigValidationError(f"Invalid source type. Must be one of: {', '.join(valid_sources)}")

        if not input_path:
            raise ConfigValidationError("Input path or query cannot be empty")

        if destination not in valid_destinations:
            raise ConfigValidationError(f"Invalid destination. Must be one of: {', '.join(valid_destinations)}")