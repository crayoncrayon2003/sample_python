# scripts/core/orchestrator/task_runner.py

from prefect import task, get_run_logger

# Import PipelineBuilder using a relative path to avoid circular dependencies.
# This assumes task_runner.py is in 'orchestrator' and pipeline_builder.py is in 'pipeline'.
from ..pipeline.pipeline_builder import PipelineBuilder

@task
def run_pipeline_as_task(pipeline: PipelineBuilder):
    """
    Takes a configured PipelineBuilder instance and runs its entire pipeline
    as a single, monolithic Prefect task.

    This wrapper function allows the main ETL flow definition to remain clean
    and declarative, separating the "what" (the flow definition) from the
    "how" (the pipeline execution).

    Args:
        pipeline (PipelineBuilder): A fully configured instance of the
                                    PipelineBuilder, with all steps added.
    
    Raises:
        Exception: Re-raises any exception that occurs during the pipeline
                   run, which causes the Prefect task to fail as expected.
    """
    logger = get_run_logger()
    
    try:
        # Use the name from the builder to add context to logs.
        task_run_name = f"Run Pipeline: {pipeline.name}"
        logger.info(f"--- Starting Prefect Task: {task_run_name} ---")

        # The core action: execute the pipeline.
        # All the complex logic is encapsulated within the builder's run method.
        pipeline.run()

        logger.info(f"--- Prefect Task: {task_run_name} finished successfully. ---")
    
    except Exception as e:
        logger.error(f"--- Prefect Task for pipeline '{pipeline.name}' failed! ---")
        # Re-raising the exception is crucial for Prefect to correctly
        # identify the task run as FAILED and display the error.
        raise