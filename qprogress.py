from dask.distributed import Queue, TimeoutError, get_client, Lock, Variable
from dask.distributed.utils import is_kernel
from functools import wraps

from tqdm import tqdm as con_tqdm
from tqdm.notebook import tqdm as nb_tqdm

import os
import math

def listdir_dotless(*args, **kwargs):
    return [ fn for fn in os.listdir(*args, **kwargs) if not fn.startswith('.') ]

def get_uuid():
    #
    # Return an UUID unique to this scheduler
    #
    client = get_client()

    # do we have the uuid cached?
    if getattr(client, '_qprogress_uu', None) is not None:
        return client._qprogress_uu

    client._qprogress_uu = client.scheduler_info()['id']
    return client._qprogress_uu

#    # generate a cluster-wide UUID (and cache locally)
#    with Lock('qprogress-uuid-lock'):
#        uu = client.get_metadata('qprogress.uuid', None)
#        if uu is None:
#            import uuid
#            uu = str(uuid.uuid4())
#            client.set_metadata('qprogress.uuid', uu)
#        client._qprogress_uu = uu
#        return uu

import redis
class RedisQueue:
    """Simple Queue with Redis Backend"""
    def __init__(self, name=None, _namespace=None, _redis=None):
        # if our namespace hasn't been given, find one on the cluster
        if _namespace is None:
            uuid = get_uuid()
            _namespace = f"qprogress:{uuid}"

        # if redis connection info hasn't been given, find one from the cluster
        if _redis is None:
            _redis = get_client().get_metadata('qprogress.redis', {})

        self.__db = redis.Redis(**_redis)

        if name is None:
            import uuid
            name = str(uuid.uuid4())
        self.name = name
        self.key = f'{_namespace}:{name}'
        #print(f'key: {self.key}')

        # grab redis version
        self.__redis_version = rv = list(map(int, self.__db.info('server')['redis_version'].split('.')))
        if rv[0] >= 6:
            # See https://github.com/redis/redis-rb/issues/977
            self._ceil = lambda self, v: v

    def _ceil(self, v):
        # note: this gets overridden by the constructor for redis server
        # versions 6+, as BLPOP supports fractional timeouts
        #
        # See https://github.com/redis/redis-rb/issues/977
        return int(math.ceil(v))

    def qsize(self):
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def put(self, item):
        """Put item into the queue."""
        self.__db.rpush(self.key, self._serialize(item))

    def get(self, timeout=None, batch=False):
        """Remove and return an item from the queue. 

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""

        tm = 0 if timeout is None else self._ceil(timeout)
        block = timeout is None or timeout > 0

        if not batch:
            #print(f"Here block={block}")
            if block:
                item = self.__db.blpop(self.key, timeout=tm)
                #print("blpop", item)
                if item is not None:
                    item = item[1]
            else:
                item = self.__db.lpop(self.key)
                #print("lpop", item)

            if item is None:
                # no new messages arrived within the requested timeout
                raise TimeoutError()

            return self._deserialize(item)
        else:
            # grab anything we have
            items = self.__db.pipeline().lrange(self.key, 0, -1).delete(self.key).execute()
#            print("XXX", items)
            if items[0]:
                return [ self._deserialize(item) for item in items[0] ]

            # don't have anything right now... wait for one new object
            if block:
                return [ self.get(timeout=timeout, batch=False) ]
            else:
                raise TimeoutError()

    def _serialize(self, msg):
        import pickle
        return pickle.dumps(msg, -1)

    def _deserialize(self, data):
        import pickle
        return pickle.loads(data)

class FilesystemQueue:
    #
    # WARNING: THIS CLASS IS NOT THREAD SAFE (!!!)
    #
    # IT ALSO ONLY WORKS ON SINGLE-MACHINE CLUSTERS
    #

    def _gen_tempdir_name(self, name):
        # Generate temporary directory name
        import tempfile, getpass, uuid
        if os.path.isdir('/dev/shm'):
            tempdir = '/dev/shm'
        else:
            tempdir = tempfile.gettempdir()
        dir = os.path.join(tempdir, getpass.getuser() + '-filesystemqueue-' + str(uuid.uuid4()) + "-" + name)
        return dir

    def _get_dir(self, name):
        # Get or generate a directory that will be used
        # as backing store for message passing.
        with Lock(f'FilesystemQueue-{name}') as lock:
            dir_var = Variable('FilesystemQueue_dir')
            try:
                dir = dir_var.get(timeout=0)
            except TimeoutError:  ## Note: I hacked /epyc/ssd/users/mjuric/miniconda3/envs/lsd2/lib/python3.9/site-packages/distributed/variable.py:84 to 'return {"type": "msgpack", "value": None }' to make this work
                dir = None

            if dir is None:
                dir = self._gen_tempdir_name(name)

                # Set it for other workers
                dir_var.set(dir)

        return dir

    def __init__(self, name=None, debug=False, _local=False):
        import uuid

        if name is None:
            name = str(uuid.uuid4())
        self.name = name

        self.dir = self._get_dir(name) if not _local else self._gen_tempdir_name(name)
        try:
            os.mkdir(self.dir, mode=0o700)
        except FileExistsError:
            pass

        self.msgnum = 0
        uu = str(uuid.uuid4())
        self.prefix = os.path.join(self.dir, uu)
        self.tmpfn = os.path.join(self.dir, '.' + uu)

        # for debugging
        self._debug = debug
        if self._debug:
            self._trash = os.path.join(self.dir, '.trash')
            #print(f"Trash: {self._trash}")
            try:
                os.mkdir(self._trash, mode=0o700)
                
            except FileExistsError:
                pass

    def put(self, val):
        import pickle

        fn = f'{self.prefix}-{self.msgnum:012d}'
        self.msgnum += 1

        #print(f"{fn} <- {val}")
        with open(self.tmpfn, 'wb') as fp:
            pickle.dump(val, fp, -1)

        assert not os.path.exists(fn)
        os.rename(self.tmpfn, fn)

    def get(self, timeout=None, batch=False):
        import time, pickle

        # wait for messages until timeout
        dt = timeout if timeout is not None else 0.1
        files = listdir_dotless(self.dir)
        while True:
            if files:
                break

            time.sleep(dt)
            files = listdir_dotless(self.dir)
            
            if timeout is not None:
                break

        # if we're here and there are no files, we've timed out
        if not files:
            raise TimeoutError

        # read accumulated messages
        msgs = []
        for fname in files:
            fn = os.path.join(self.dir, fname)
            with open(fn, "rb") as fp:
                msg = pickle.load(fp)

            if not self._debug:
                os.unlink(fn)
            else:
                os.rename(fn, os.path.join(self._trash, fname))

            if not batch:
                return msg
            msgs.append(msg)

        return msgs

    def qsize(self):
        return len(listdir_dotless(self.dir))

#Queue = FilesystemQueue
Queue = RedisQueue

# See https://stackoverflow.com/questions/19447603/how-to-kill-a-python-child-process-created-with-subprocess-check-output-when-t
# for how this ensures redis is cleanly cleaned up when we terminate
import signal
import ctypes
libc = ctypes.CDLL("libc.so.6")
def set_pdeathsig(sig = signal.SIGTERM):
    def callable():
        return libc.prctl(1, sig)
    return callable

def _redis_server(pwd):
    # launch redis-server
    import subprocess
    ls_output=subprocess.Popen(["redis-server", "-"], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, preexec_fn=set_pdeathsig(signal.SIGINT))
    cfg = f"requirepass '{pwd}'\ndaemonize no".encode('utf-8')
#    print("===================")
#    print(cfg)
#    print("===================")
    ret = ls_output.communicate(cfg)

_backend_initialized = False
def _init_backend(client=None):
    global _backend_initialized
    if _backend_initialized:
        return

    if Queue == RedisQueue:
        # autmatically start redis and store the connection info in
        # the cluster metadata
        
        # 1. generate redis password
        import secrets
        password_length = 16
        redispwd = secrets.token_urlsafe(password_length)
        print(f"redis password: {redispwd}")

        # 2. launch redis & wait until up
        import threading
        threading.Thread(target=_redis_server, args=(redispwd,), daemon=True).start()

        r = redis.Redis(password=redispwd)
        for _ in range(50):
            try:
                print("-------------XXXXXXXXXXXXX Ping")
                r.ping()
                print("-------------XXXXXXXXXXXXX OK")
                break
            except redis.ConnectionError:
                print("+++++++++++++ connection error")
                import time
                time.sleep(0.1)
        else:
            raise Exception("Couldn't connect to redis")
        print("XXXXXXXXXXXXXXXXX")

        # 3. store password as dask cluster metadata
        if client is not None:
            client.set_metadata('qprogress.redis', dict(password=redispwd))

        # 4. flip our "initialized" flag
        _backend_initialized = True

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

class show_progress:
    def __init__(self, f, key="default"):
        self.f, self.key = f, key

    def __call__(self, *args, **kwargs):
        ret = self.f(*args, **kwargs)
        log(self.key, 1)
        return ret

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

    client = get_client()
    _init_backend(client)

    fut = client.compute(b)

    q = Queue('_default_q')

    _timeout = 0.1  # internal timeout
    while True:
        try:
            # check if we're done. we do this first to
            # ensure the queue will be drained _after_ the
            # future is marked as done
            done = fut.done()
            if done:
                # no need to wait if no new messages are available
                _timeout = 0

            # wait for a next message in all queues
            msgs = q.get(timeout=_timeout, batch=True)
            #print("XXXXXXXXXXXX:", msgs)
            for key, msg in msgs:
                yield key, msg
        except TimeoutError:
            pass

        if done:
            break

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

