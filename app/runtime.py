"""In-memory per-batch runtime: cancellation and in-flight writer tracking."""
import asyncio


class BatchRuntime:
    def __init__(self):
        self.cancel = asyncio.Event()
        self.cond = asyncio.Condition()
        self.active = 0                              # in-flight writers (uploads + bg tasks)
        self.end_uploading = {"r1": False, "r2": False}
        self.progress = {"r1": 0, "r2": 0}           # live received bytes per end
        self.pair_lock = asyncio.Lock()
        self.term_lock = asyncio.Lock()


class Runtimes:
    def __init__(self):
        self._map = {}

    def get(self, batch_id):
        return self._map.get(batch_id)

    def add(self, batch_id) -> BatchRuntime:
        rt = BatchRuntime()
        self._map[batch_id] = rt
        return rt

    def pop(self, batch_id):
        self._map.pop(batch_id, None)
