from inspect_ai import Task, task
from inspect_ai.dataset import Sample
from inspect_ai.scorer import exact
from inspect_ai.solver import generate, use_tools
from inspect_ai.tool import bash_session

@task
def custom_sandbox():
    return Task(
        dataset=[
            Sample(
                input="Read the contents of main.py and reply with just the output of running it.",
                target=value,
                setup=f"git checkout {ref}",
            )
            for ref, value in (
                ("main", "This is the third message"),
                ("main~1", "This is the second message"),
                ("main~2", "Hello, World!"),
            )
        ],
        solver=[use_tools(bash_session()), generate()],
        scorer=exact(),
        sandbox=("docker", "compose.yaml"),
    )
