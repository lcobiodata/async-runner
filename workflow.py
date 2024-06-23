import asyncio
import time
from types import SimpleNamespace
import json
import networkx as nx
import matplotlib.pyplot as plt

def upstream(*dependencies):
    def decorator(func):
        func.upstream = dependencies
        return func
    return decorator

class AsyncRunner:
    def __init__(self, tasks):
        self.results = SimpleNamespace()
        self.futures = SimpleNamespace()
        self.tasks = tasks

    async def _run_task(self, task_name):
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
        all_tasks = [task for task in dir(self.tasks) if not task.startswith('__') and hasattr(getattr(self.tasks, task), 'upstream')]
        all_deps = [dep for task in all_tasks if hasattr(getattr(self.tasks, task), 'upstream') for dep in getattr(self.tasks, task).upstream]
        ultimate_task = next(task for task in all_tasks if task not in all_deps)
        await self._run_task(ultimate_task)


if __name__ == "__main__":
    # Define a function that takes the task object and plots a dependency graph
    def plot_dependency_graph(tasks):
        G = nx.DiGraph()
        for task in dir(tasks):
            if not task.startswith('__') and hasattr(getattr(tasks, task), 'upstream'):
                for dep in getattr(tasks, task).upstream:
                    G.add_edge(dep, task)
        pos = nx.kamada_kawai_layout(G)
        nx.draw(G, pos, with_labels=True, node_size=3000, node_color="skyblue", font_size=10, font_weight="bold", font_color="black", edge_color="gray", linewidths=1, arrowsize=20)
        plt.show()

    class Tasks:
        @upstream()
        async def task_1(self):
            print("Task 1: Making API call")
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 1 Result"

        @upstream('task_1')
        async def task_2(self, task_1_result):
            print("Task 2: Making API call with data:", task_1_result)
            await asyncio.sleep(10)  # Simulating API call
            return "Task 2 Result"

        @upstream('task_1')
        async def task_3(self, task_1_result):
            print("Task 3: Making API call with data:", task_1_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 3 Result"

        @upstream('task_2', 'task_3')
        async def task_4(self, task_2_result, task_3_result):
            print("Task 4: Making API call with data:", task_2_result, task_3_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 4 Result"

        @upstream('task_4')
        async def task_5(self, task_4_result):
            print("Task 5: Making API call with data:", task_4_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 5 Result"

        @upstream('task_4')
        async def task_6(self, task_4_result):
            print("Task 6: Making API call with data:", task_4_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 6 Result"

        @upstream('task_4')
        async def task_7(self, task_4_result):
            print("Task 7: Making API call with data:", task_4_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 7 Result"

        @upstream('task_5', 'task_6', 'task_7')
        async def task_8(self, task_5_result, task_6_result, task_7_result):
            print("Task 8: Making API call with data:", task_5_result, task_6_result, task_7_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 8 Result"

        @upstream('task_8')
        async def task_9(self, task_8_result):
            print("Task 9: Making API call with data:", task_8_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 9 Result"

        @upstream('task_8')
        async def task_10(self, task_8_result):
            print("Task 10: Making API call with data:", task_8_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 10 Result"
        
        @upstream('task_1', 'task_3')
        async def task_11(self, task_1_result, task_3_result):
            print("Task 11: Making API call with data:", task_1_result, task_3_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 11 Result"
        
        @upstream('task_2', 'task_5')
        async def task_12(self, task_2_result, task_5_result):
            print("Task 12: Making API call with data:", task_2_result, task_5_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 12 Result"
        
        @upstream('task_3', 'task_8')
        async def task_13(self, task_3_result, task_8_result):
            print("Task 13: Making API call with data:", task_3_result, task_8_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 13 Result"

        @upstream('task_1')
        async def task_14(self, task_1_result):
            print("Task 14: Making API call with data:", task_1_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 14 Result"
        
        @upstream('task_7')
        async def task_15(self, task_7_result):
            print("Task 15: Making API call with data:", task_7_result)
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return "Task 15 Result"

        @upstream('task_9', 'task_10', 'task_11', 'task_12', 'task_13', 'task_14', 'task_15')
        async def main(self, task_9_result, task_10_result, task_11_result, task_12_result, task_13_result, task_14_result, task_15_result):
            print("Awaiting task 9, task 10, task 11, task 12, task 13, task 14, task 15")
            trigger = asyncio.sleep(0.1) # Simulating API call
            await asyncio.sleep(0.1)  # Simulating some other work
            await trigger
            await asyncio.sleep(0.1)  # Simulating some other work
            return f"Main Task Result: {task_9_result}, {task_10_result}, {task_11_result}, {task_12_result}, {task_13_result}, {task_14_result}, {task_15_result}"

    tasks = Tasks()
    workflow = AsyncRunner(tasks)
    from viztracer import VizTracer
    output_file = "output.json"
    # Use VizTracer to trace the execution of the function
    with VizTracer(
        output_file=output_file,
        ignore_c_function=True
    ) as tracer:
        asyncio.run(workflow.run())
    print("Results:\n", json.dumps(workflow.results.__dict__, indent=2))
    # Plot the dependency graph
    plot_dependency_graph(tasks)