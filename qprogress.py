from dask.distributed import Queue, TimeoutError, get_client
from dask.distributed.utils import is_kernel
from functools import wraps

from tqdm import tqdm as con_tqdm
from tqdm.notebook import tqdm as nb_tqdm

class CaptureReturnValueGenerator:
    # Wraps a generator to store its return value in
    # self._retval. See https://stackoverflow.com/a/34073775 for
    # inspiration.
    def __init__(self, gen):
        self.gen = gen
        self.retval = None

    def __iter__(self):
        self.retval = yield from self.gen

def capture_value(f):
    # Function wrapper to capture the value of
    # function-based generators
    @wraps(f)
    def g(*args, **kwargs):
        return CaptureReturnValueGenerator(f(*args, **kwargs))
    return g

@capture_value
def _communicate(b, *queues):
    # intented to be used by class progress.
    #
    # given a dask-computable object b, and dask.Queue objects queues,
    # start computing b and listen to the queues for any messages. Yield
    # back the messages as they arrive, until the object b is computed.
    #
    # if len(queues) == 1, yield the messages
    # if len(queues) >1, yield a tuple of queue on which the message was
    # received, and a batch of received messages.

    fut = get_client().compute(b)

    timeout = 0.1  # internal timeout
    while True:
        try:
            # check if we're done. we do this first to
            # ensure the queue will be drained _after_ the
            # future is marked as done
            done = fut.done()

            # wait for a next message in all queues
            if len(queues) == 1:
                msgs = queues[0].get(timeout=timeout, batch=True)
                yield from msgs
            else:
                for q in queues:
                    msgs = q.get(timeout=timeout, batch=True)
                    yield q, msgs
            if done:
                break
        except TimeoutError:
            pass

    for q in queues:
        assert q.qsize() == 0

    return fut

class progress:
    _prog = None

    def __init__(self, b, *queues, tqdm=None, **kwargs):
        #
        # Display one or more progress bars to display the progress of execution of b via
        # messagess arriving on queues.
        #
        # b: dask-computable object to compute
        # queues: the queues on which to listen for messages until b is computed
        # tqdm: tqdm-compatible class to use for progress bar (None autoselect an apropriate one)
        #
        # kwargs are passed on to tqdm's constructor. If there are multiple queues, then
        # any kwargs with a number suffix are passed into only that tqdm bar's constructor.
        # (example: length=10 would be passed to all progress bars, but length2=10 only to
        # progress bar #2). The counting starts at 1.
        #
        self._gen = _communicate(b, *queues)
        self._queues = queues
        self._retval = None

        # select tqdm class
        if tqdm is None:
            notebook = is_kernel()
            tqdm = con_tqdm if not notebook else nb_tqdm
        self.tqdm = tqdm

        # set up progres bar configs
        self._configs = {}
        for i, q in enumerate(queues):
            cfg = self._configs[q.name] = dict(position=i)

            # apply kwargs configs
            suffix = str(i+1)
            for kw, val in kwargs.items():
                if kw.endswith(suffix):
                    kw = kw[:-len(suffix)]
                elif kw[-1].isdigit():
                    continue
                cfg[kw] = val

    def __iter__(self):
        if len(self._queues) == 1:
            # a single progress bar is simple & fast
            self._prog = self.tqdm(self._gen, **next(iter(self._configs.values())))
            yield from self._prog
            self._prog.refresh()
            self._prog.close()
        else:
            # create the progress bars
            pbars = {}
            #print(self._configs)
            for k, vals in self._configs.items():
                pbars[k] = self.tqdm(**vals)

            # yield from the iterator
            # in this variant, self._gen returns q, [msg1, msg2, ...]
            for q, msgs in self._gen:
                self.q = q
                self._prog = pbars[q.name]
                for msg in msgs:
                    self._prog.update()
                    yield msg

            # close the progress bars
            for pbar in pbars.values():
                pbar.refresh()
                pbar.close()

        self._retval = self._gen.retval

    def result(self):
        # return computation result
        return self._retval.result()

    def __getattr__(self, name):
        # dispatch to the current progress bar
        return getattr(self._prog, name)

def compute_with_progress(b, *queues, **kwargs):
    # run the computation displaying a default progress bar.
    # return the result.
    prog = progress(b, *queues, **kwargs)
    for _ in prog:
        pass
    return prog.result()

