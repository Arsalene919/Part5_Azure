import logging
import azure.functions as func
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    # Step 1: Read input files and set container name
    container_name = "durablefunction"
    input_files = ["mrinput-1.txt", "mrinput-2.txt", "mrinput-3.txt", "mrinput-4.txt"]

    # Step 2: Call the MapperActivity for each input file in parallel
    parallel_task = [
        context.call_activity("MasterActivity", {"file": file, "container": container_name})
        for file in input_files
    ]

    # Wait for all MapperActivity tasks to complete
    map_results = yield context.task_all(parallel_task)

    # Step 3: Call ShufflerActivity
    shuffle_result = yield context.call_activity("ShufflerActivity", map_results)

    # Step 4: Call ReducerActivity for each key in the shuffled results
    reducer_tasks = [
        context.call_activity("ReducerActivity", {"key": key, "values": values})
        for key, values in shuffle_result.items()
    ]

    # Wait for all ReducerActivity tasks to complete
    reduce_results = yield context.task_all(reducer_tasks)

    return reduce_results


# Define the entry point for the orchestrator
main = df.Orchestrator.create(orchestrator_function)
