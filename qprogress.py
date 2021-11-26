from dask.distributed import Queue, TimeoutError, get_client
from dask.distributed.utils import is_kernel
from functools import wraps

from tqdm import tqdm as con_tqdm
from tqdm.notebook import tqdm as nb_tqdm

def log(*args):
    # infer message key
    if len(args) == 1:
        key = 'default'
    else:
        key, args = args[0], args[1:]

    # get the queue object
    import dask
    try:
        q = dask._default_q
    except AttributeError:
        q = dask._default_q = Queue('_default_q')

    # post the message
    q.put((key, args[0]))

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
def _communicate(b):
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

    q = Queue('_default_q')

    _timeout = 0.1  # internal timeout
    while True:
        try:
            # check if we're done. we do this first to
            # ensure the queue will be drained _after_ the
            # future is marked as done
            done = fut.done()

            # wait for a next message in all queues
            msgs = q.get(timeout=_timeout, batch=True)
            #print("XXXXXXXXXXXX:", msgs)
            for key, msg in msgs:
                yield key, msg

            if done:
                break
        except TimeoutError:
            pass

    assert q.qsize() == 0

    return fut

class progress:
    _prog = None

    def __init__(self, b, keys=set(), tqdm=None, kv=False, **kwargs):
        #
        # Display one or more progress bars to display the progress of execution of b via
        # messagess arriving on queues.
        #
        # b: dask-computable object to compute
        # queues: the queues on which to listen for messages until b is computed
        # tqdm: tqdm-compatible class to use for progress bar (None autoselect an apropriate one)
        # kv: return msg value or (k, v) tuple
        #
        # kwargs are passed on to tqdm's constructor. If there are multiple queues, then
        # any kwargs with a number suffix are passed into only that tqdm bar's constructor.
        # (example: length=10 would be passed to all progress bars, but length2=10 only to
        # progress bar #2). The counting starts at 1.
        #
        self._gen = _communicate(b)
        self._retval = None
        self._kv = kv
        self._keys = set(keys)

        # select tqdm class
        if tqdm is None:
            notebook = is_kernel()
            tqdm = con_tqdm if not notebook else nb_tqdm
        self.tqdm = tqdm

        # set up progres bar configs
        self._configs = {}
        self._config_default = kwargs

    def config(self, name, **kwargs):
        cfg = self._configs[name] = self._config_default.copy()
        cfg.update(kwargs)
        return self

    def _new_pbar(self, name, position):
        try:
            cfg = self._configs[name]
        except KeyError:
            cfg = self._config_default.copy()

        if 'desc' not in cfg:
            cfg['desc'] = name if name != 'default' else "Progress"

        return self.tqdm(position=position, **cfg)

    def __iter__(self):
        # start with the pre-defined progress bars,
        # and only those listed in self._keys (if non-empty)
        keys = self._keys
        if not keys:
            configs = self._configs
        else:
            # trim configs to allowed keys only
            configs = { k: v for k, v in self._configs.items() if k in keys }
        # create new progress bars, from the given configs
        pbars = {}
        for i, name in enumerate(configs):
            pbars[name] = self._new_pbar(name, i)

        # yield from the iterator, update/create progress bars
        # as necessary.
        for name, msg in self._gen:
            # get or create a new progress bar
            try:
                self._prog = pbars[name]
            except KeyError:
                if not keys or name in keys:
                    # dynamically create new progress bars unless filtered
                    # by keys
                    self._prog = pbars[name] = self._new_pbar(name, len(pbars))
                else:
                    continue

            # update the progress, yield the message
            # and message key
            self._prog.update()
            yield (name, msg) if self._kv else msg

        # close the progress bars
        for pbar in pbars.values():
            pbar.close()

        self._retval = self._gen.retval

    def result(self):
        # return computation result
        return self._retval.result()

    def __getattr__(self, name):
        # dispatch to the current progress bar
        return getattr(self._prog, name)

def compute_with_progress(b, config={}, **kwargs):
    # run the computation displaying a default progress bar.
    # return the result.
    prog = progress(b, **kwargs)
    for k, cfg in config.items():
        prog.config(k, **cfg)
    for _ in prog:
        pass
    return prog.result()

