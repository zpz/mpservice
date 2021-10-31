import concurrent.futures


class BackgroundTask:
    def __init__(self, executor: concurrent.futures.Executor = None):
        pass

    def submit(self, *args, **kwargs) -> str:
        pass

    def submit_and_forget(self, *args, **kwargs) -> str:
        pass

    def get_task_id(self, *args, **kwargs) -> str:
        pass

    def exists(self, task_id: str) -> bool:
        pass

    def cancel(self, task_id):
        pass

    def cancelled(self, task_id: str) -> bool:
        pass

    def running(self, task_id: str) -> bool:
        pass

    def done(self, task_id: str) -> bool:
        pass

    def result(self, task_id: str):
        pass

    def exception(self, task_id: str):
        pass
