# scripts/tasks/transform/with_jinja2.py
from pathlib import Path
from typing import Any, Dict

import duckdb
import pandas as pd
from jinja2 import Environment, FileSystemLoader
from prefect import task


@task
def transform_with_jinja2(
    input_path: str,
    working_dir: Path,
    template_path: Path,
    output_filename: str,
    **template_params: Any,
) -> str:
    """
    Generates a text-based file from structured data using a Jinja2 template.

    This task reads a Parquet file, converts its contents into a list of
    dictionaries, and then uses this data to render a Jinja2 template.
    The rendered content is saved to a new file. The data is available
    within the template under the 'data' variable.

    Args:
        input_path: The path to the source Parquet file.
        working_dir: The directory to save the generated output file.
        template_path: The path to the Jinja2 template file.
        output_filename: The name for the output file.
        **template_params: Keyword arguments passed to the template for rendering.
                           These are available as top-level variables.

    Returns:
        The path to the newly created output file as a string.
    """
    print(f"Jinja2 Transform: Rendering '{template_path.name}' with data from '{Path(input_path).name}'...")

    # Step 1: Read the Parquet file and convert it to a list of dictionaries.
    # This format is easy to iterate over in Jinja2 templates.
    con = duckdb.connect(database=":memory:")
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{input_path}')").fetchdf()
    finally:
        con.close()

    data_for_template: list[dict] = df.to_dict(orient="records")

    # Step 2: Render the Jinja2 template.
    env = Environment(loader=FileSystemLoader(str(template_path.parent)), autoescape=True)
    template = env.get_template(template_path.name)

    # Combine the main data with any additional parameters for rendering.
    render_vars = {
        "data": data_for_template,
        **template_params,
    }
    rendered_content = template.render(**render_vars)

    # Step 3: Save the rendered content to the output file.
    output_path = working_dir / output_filename
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered_content)

    print(f"Jinja2 Transform successful: Output file saved to {output_path}")
    return str(output_path)