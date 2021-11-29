import qprogress
import time
from asyncio.exceptions import TimeoutError
from dask.distributed import Client, Queue, get_client
from dask import delayed
from dask.delayed import Delayed

def produce(q, nmsg=1_000):
    for i in range(nmsg):
        msg = f'message {i}'
        q.put(msg)

def consume(q):
    try:
        msgs = q.get(timeout=0, batch=True)
    except TimeoutError:
        pass

def timed_produce(q, nmsg=1_000):
    t0 = time.perf_counter()
    produce(q, nmsg=nmsg)
    t1 = time.perf_counter()
    dt = (t1 - t0) * 1000 # milliseconds
    return dt

def timed_consume(q, nmsg=1_000):
    t0 = time.perf_counter_ns()
    consume(q)
    t1 = time.perf_counter_ns()
    dt = (t1 - t0) / 1000_0000 # milliseconds
    return dt

def test(q):
    q.put("test message")
    q.put("another test message")

    msgs = q.get(timeout=0.1, batch=True)
    print(msgs)

    try:
        msgs = q.get(timeout=0.1, batch=True)
        print(msgs)
    except TimeoutError:
        print("Caught TimeoutError")

    q.put("test message 2")
    q.put("another test message 2")

    msg = q.get(timeout=0)
    print("MSG1:", msg)
    msg = q.get()
    print("MSG2:", msg)

    try:
        msg = q.get(timeout=0)
        print("MSG3:", msg)
    except TimeoutError:
        print("Caught TimeoutError")

    try:
        msg = q.get(timeout=2.5)
        print("MSG3:", msg)
    except TimeoutError:
        print("Caught TimeoutError")

if __name__ == "__main__":

#    q = qprogress.FilesystemQueue("default", _local=True)

    cfg = qprogress.init(_class=qprogress.RedisQueue)
    q = qprogress.RedisQueue("default", _namespace='default', _redis=cfg)

#    nworkers = 20
#    client = Client(n_workers=nworkers, threads_per_worker=1)
#    q = Queue("default")
#    timed_produce = delayed(timed_produce)
#    timed_consume = delayed(timed_consume)

    print(q.name)

    if False:
        test(q)

    if True:
        nmsg=5_000

        dt = timed_produce(q, nmsg)    
        if isinstance(dt, Delayed):
            dt = dt.compute()
        print(f"Produce ran for {dt} milliseconds, {1000/(dt/nmsg):,.0f} msgs/sec.")

        print(f"{q.qsize()} messages in queue.")

        dt = timed_consume(q, nmsg)
        if isinstance(dt, Delayed):
            dt = dt.compute()
        print(f"Consume ran for {dt} milliseconds, {1000/(dt/nmsg):,.0f} msgs/sec.")
