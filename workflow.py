import asyncio

def upstream(*dependencies):
    """
    Decorator to specify upstream dependencies for a task.
    :param dependencies: List of upstream dependencies for the task.
    :return: Decorator function.
    """
    def decorator(func):
        func.upstream = dependencies
        return func
    return decorator

class AsyncRunner:
    """
    Class to run a workflow of asynchronous tasks with dependencies.
    """
    def __init__(self, tasks):
        self.results = SimpleNamespace()
        self.futures = SimpleNamespace()
        self.tasks = tasks

    async def _run_task(self, task_name):
        """
        Run a task and its upstream dependencies.
        :param task_name: Name of the task to run.
        """
        task = getattr(self.tasks, task_name)
        if not hasattr(self.futures, task_name):
            print(f"Running task: {task_name} not in futures")
            setattr(self.futures, task_name, asyncio.Future())
            await asyncio.gather(*(self._run_task(dep) for dep in task.upstream))
            results = [self.results.__dict__[dep] for dep in task.upstream]
            result = await task(*results)
            setattr(self.results, task_name, result)
            getattr(self.futures, task_name).set_result(None)
        else:
            print(f"Running task: {task_name} already in futures")
            await getattr(self.futures, task_name)

    async def run(self):
        """
        Run the workflow by starting from the task with no dependencies.
        """
        all_tasks = [task for task in dir(self.tasks) if not task.startswith('__') and hasattr(getattr(self.tasks, task), 'upstream')]
        all_deps = [dep for task in all_tasks if hasattr(getattr(self.tasks, task), 'upstream') for dep in getattr(self.tasks, task).upstream]
        ultimate_task = next(task for task in all_tasks if task not in all_deps)
        await self._run_task(ultimate_task)


if __name__ == "__main__":
    import random
    import time
    from types import SimpleNamespace
    import json
    import networkx as nx
    import matplotlib.pyplot as plt

    def simulate_api_call(min_sleep=0.5, max_sleep=2.0):
        sleep_time = random.uniform(min_sleep, max_sleep)
        print(f"Simulating API call, sleeping for {sleep_time:.2f} seconds")
        return asyncio.sleep(sleep_time)

    def plot_dependency_graph(tasks):
        G = nx.DiGraph()
        
        # Build the graph based on task dependencies
        for task in dir(tasks):
            if not task.startswith('__') and hasattr(getattr(tasks, task), 'upstream'):
                for dep in getattr(tasks, task).upstream:
                    G.add_edge(dep, task)
        
        # Assign a layer to each node based on its depth in the graph
        for node in nx.topological_sort(G):
            G.nodes[node]['subset'] = len(nx.ancestors(G, node))
        
        # Export to GraphML file
        nx.write_graphml(G, "workflow.graphml")
        
        # Use the dot layout for a top-bottom hierarchical view
        pos = nx.nx_pydot.graphviz_layout(G, prog="dot")
        
        plt.figure(figsize=(12, 8))
        nx.draw(G, pos, with_labels=True, node_size=3000, node_color="skyblue", font_size=10, font_weight="bold", font_color="black", edge_color="gray", linewidths=1, arrowsize=20)
        plt.title("Task Dependency Graph")
        plt.show()


    class Pipeline:
        @upstream()
        async def start_pipeline(self):
            print("Starting pipeline")
            await simulate_api_call()
            return "Pipeline Started"

        @upstream('start_pipeline')
        async def extract_data_a(self, initial_setup):
            print("Extracting data from source A")
            await simulate_api_call()
            return {"data_a": "raw data A"}

        @upstream('start_pipeline')
        async def extract_data_b(self, initial_setup):
            print("Extracting data from source B")
            await simulate_api_call()
            return {"data_b": "raw data B"}

        @upstream('extract_data_a')
        async def clean_data_a(self, extracted_data_a):
            print("Cleaning data A:", extracted_data_a)
            await simulate_api_call()
            return {"data_a": "cleaned data A"}

        @upstream('extract_data_b')
        async def clean_data_b(self, extracted_data_b):
            print("Cleaning data B:", extracted_data_b)
            await simulate_api_call()
            return {"data_b": "cleaned data B"}

        @upstream('clean_data_a')
        async def transform_data_a(self, cleaned_data_a):
            print("Transforming data A:", cleaned_data_a)
            await simulate_api_call()
            return {"data_a": "transformed data A"}

        @upstream('clean_data_b')
        async def transform_data_b(self, cleaned_data_b):
            print("Transforming data B:", cleaned_data_b)
            await simulate_api_call()
            return {"data_b": "transformed data B"}

        @upstream('transform_data_a', 'transform_data_b')
        async def merge_data(self, transformed_data_a, transformed_data_b):
            print("Merging data A and B:", transformed_data_a, transformed_data_b)
            await simulate_api_call()
            return {"data": "merged data"}

        @upstream('merge_data')
        async def load_data(self, merged_data):
            print("Loading data:", merged_data)
            await simulate_api_call()
            return {"data": "loaded data"}

        @upstream('load_data')
        async def analyze_data(self, loaded_data):
            print("Analyzing data:", loaded_data)
            await simulate_api_call()
            return {"data": "analyzed data"}

        @upstream('load_data')
        async def validate_data(self, loaded_data):
            print("Validating data:", loaded_data)
            await simulate_api_call()
            return {"data": "validated data"}

        @upstream('load_data')
        async def archive_data(self, loaded_data):
            print("Archiving data:", loaded_data)
            await simulate_api_call()
            return {"data": "archived data"}

        @upstream('transform_data_a')
        async def additional_processing_a(self, transformed_data_a):
            print("Additional processing A:", transformed_data_a)
            await simulate_api_call()
            return {"data_a": "additional processed data A"}

        @upstream('transform_data_b')
        async def additional_processing_b(self, transformed_data_b):
            print("Additional processing B:", transformed_data_b)
            await simulate_api_call()
            return {"data_b": "additional processed data B"}

        @upstream('additional_processing_a', 'additional_processing_b')
        async def integrate_additional_data(self, additional_data_a, additional_data_b):
            print("Integrating additional data A and B:", additional_data_a, additional_data_b)
            await simulate_api_call()
            return {"data": "integrated additional data"}

        @upstream('analyze_data', 'validate_data', 'integrate_additional_data')
        async def generate_report(self, analyzed_data, validated_data, integrated_additional_data):
            print("Generating report with data:", analyzed_data, validated_data, integrated_additional_data)
            await simulate_api_call()
            return {"report": "data analysis report"}

        @upstream('generate_report', 'archive_data')
        async def main(self, report, archived_data):
            print("Finalizing and saving report:", report)
            print("Archiving data:", archived_data)
            await simulate_api_call()
            return "Pipeline Completed"

    tasks = Pipeline()
    workflow = AsyncRunner(tasks)

    from viztracer import VizTracer
    output_file = "output.json"
    with VizTracer(
        output_file=output_file,
        ignore_c_function=True
    ) as tracer:
        asyncio.run(workflow.run())
    print("Results:\n", json.dumps(workflow.results.__dict__, indent=2))

    plot_dependency_graph(tasks)
