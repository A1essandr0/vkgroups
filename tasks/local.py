from invoke import task

@task()
def run(context):
    """Run application locally"""
    cmd = f"cd app && poetry run python main.py"
    context.run(cmd)