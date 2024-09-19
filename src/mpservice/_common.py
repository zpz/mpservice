import builtins


class TimeoutError(builtins.TimeoutError):
    pass


class StopRequested(Exception):
    pass
