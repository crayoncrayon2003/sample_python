# scripts/utils.py
from pathlib import Path
from typing import Any, Dict

import yaml
from jinja2 import Environment, FileSystemLoader


def load_config(path: Path) -> Dict[str, Any]:
    """
    Loads a YAML configuration file from the given path.

    Args:
        path: The Path object pointing to the YAML configuration file.

    Returns:
        A dictionary containing the configuration.
    """
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def load_sql_template(template_path: Path, **kwargs: Any) -> str:
    """
    Renders an SQL query from a Jinja2 template.

    Args:
        template_path: The Path object pointing to the Jinja2 template file.
        **kwargs: Keyword arguments to pass to the template for rendering.

    Returns:
        A string containing the rendered SQL query.
    """
    # Set up the Jinja2 environment with the directory containing the template
    env = Environment(loader=FileSystemLoader(str(template_path.parent)))

    # Get the template and render it with the provided variables
    template = env.get_template(template_path.name)
    return template.render(**kwargs)