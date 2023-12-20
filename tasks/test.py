from invoke import task


@task()
def run(context):
    """Run unit tests"""
    cmd = f"cd tests && poetry run pytest -vv --asyncio-mode=auto"
    context.run(cmd)

@task()
def run_mypy(context):
    """Run mypy"""
    cmd = f"cd app && poetry run mypy main.py"
    context.run(cmd)
