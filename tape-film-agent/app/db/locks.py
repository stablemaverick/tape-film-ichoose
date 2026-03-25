from contextlib import contextmanager


@contextmanager
def job_lock(_name: str):
    # Placeholder: wire to DB advisory lock or lock table.
    yield
