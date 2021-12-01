import hipscat.progress as qprogress
import time
from asyncio.exceptions import TimeoutError
from dask.distributed import Client, Queue, get_client, wait, Event
import dask
import numpy as np

import uuid
uid = str(uuid.uuid4())

def produce(q, nmsg=1_000):
    for i in range(nmsg):
        msg = f'{uid}::message {i}'
        q.put(msg)

def consume(q):
    try:
        return q.get(timeout=0, batch=True)
    except TimeoutError:
        return []

@dask.delayed
def timed_produce(Queue, name, nmsg=1_000):
    q = Queue(name)

    # wait until the consumer is ready
    e = Event('start')
    e.wait()

    t0 = time.perf_counter()
    produce(q, nmsg=nmsg)
    t1 = time.perf_counter()
    dt = (t1 - t0) * 1000 # milliseconds
    return dt

def timed_consume(futures, q):
    prefix = f'{uid}::message '
    prod_dt = []
    nmsg = 0

    running = futures
    tm = 0.1

    t0 = time.perf_counter()
    while len(running):
        # check if we have remaining running futures
        try:
            # Sigh....
            # dask bug #1: setting timeout=0 causes "RuntimeWarning: coroutine 'FutureState.wait' was never awaited" (and the loop never exits)
            # dask bug #2: setting timeout=0.000001 causes wait to timeout always
            # Schlamperei !!:
            _, running = wait(running, timeout=0.001, return_when='FIRST_COMPLETED')
        except TimeoutError:
            pass
        if len(running) == 0:
            tm = 0.

        # consume from the queue
        try:
            print(f"Get {len(running)=} {tm=} ... ", end='')
            msgs = q.get(timeout=tm, batch=True)
            nmsg += len(msgs)
            print(f"done {len(msgs)=} {nmsg=}")
        except TimeoutError:
            print(f"timed out.")
            for i, fut in enumerate(running):
                print(f"   {i=} {fut=}")
            continue

        for msg in msgs:
            assert msg.startswith(prefix), f"{msg=} {prefix=}"

    t1 = time.perf_counter()

    # collect results
    prod_dt += [ fut.result() for fut in futures ]

    dt = (t1 - t0) * 1000 # milliseconds
    return dt, prod_dt, nmsg

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

    nworkers = 48
    client = Client(n_workers=nworkers, threads_per_worker=1)
    print(f"Started {nworkers=} cluster")

#    q = qprogress.FilesystemQueue("default")

    qprogress.init(client=client, _class=qprogress.RedisQueue)
    q = qprogress.RedisQueue("default")

#    q = Queue("default")

    print(q.__class__, q.name)

    if False:
        test(q)

    if True:
        nmsg=40_000
        e = Event('start') # to synchronise start of producing with start of consuming

        # submit all workers
        nperworker = nmsg // nworkers
        nmsg = nperworker * nworkers
        print(f"Launching tasks:")
        futures = client.compute( [ timed_produce(q.__class__, q.name, nperworker) for _ in range(nworkers) ] )

        # Give the workers a second to get ready... (this shouldn't be needed)
        time.sleep(1)

        # and then start consuming
        print(f"Starting to consume:")
        e.set() # notify the workers we're ready to go
        dt, prod_dt, nmsg_received = timed_consume(futures, q)
        assert nmsg_received == nmsg, f"{nmsg_received=} {nmsg=}"
        med_dt = np.median(prod_dt)

        print(f"Consume ran for {dt} milliseconds, {1000/(dt/nmsg):,.0f} msgs/sec.")
        print(f"Median produce ran for {med_dt} milliseconds for {nperworker} messages, {1000/(med_dt/nperworker):,.0f} msgs/sec/worker.")
        print("Per-task timings:")
        for i, dt in enumerate(prod_dt):
            print(f"  {i=} {dt=}")
