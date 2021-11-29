import dask.bag as db
from dask.distributed import Client, Queue, get_client
import numpy as np
import qprogress
from qprogress import progress, compute_with_progress, log

def some_fun(v):
    log('prog1', int(v))
    return v

def some_fun2(v):
    log('prog2', int(v))
    return v

def get_meta():
    client = get_client()
    val = client.get_metadata('redis')
    client.set_metadata('redis', ('tuple', {'with': 'dict'}))
    return val

def test_metadata():
    client.set_metadata('redis', "localhost:6379")
    fut = client.submit(get_meta)
    print(fut.result())
    print(client.get_metadata('redis'))
    print(client.get_metadata('doesntexist', 'nada'))
    exit()

def test_redis_launch():
    qprogress.init()
    import time
    print("Will sleep...")
    time.sleep(500)
    print("Exiting...")
    exit(0)

if __name__ == "__main__":
    if False:
        test_redis_launch()

    print("Starting LocalCluster... ", end='')
    client = Client(n_workers=20, threads_per_worker=1)
    print("done.")

    import test
    vals = np.arange(2000)

    b = db.from_sequence(vals).map(some_fun).map(lambda x: [x, 2*x]).flatten().map(some_fun2)

    if False:
        test_metadata()

    if True:
        prog = progress(b, kv=True).config('prog1', total=len(vals)).config('prog2', total=2*len(vals))
        totals = dict(prog1=0, prog2=0)
        for k, v in prog:
            totals[k] += v
            prog.set_postfix_str(f'Rows loaded: {totals[k]: 12,d}')
        result = prog.result()

    if False:
#        result = compute_with_progress(b, desc_prog1=" First map", desc_prog2="Second map", total_prog1=len(vals), total_prog2=2*len(vals))
        result = compute_with_progress(b, config=dict(prog1=dict(desc=" First map", total=len(vals)), prog2=dict(desc="Second map", total=2*len(vals))))

    if False:
        from itertools import accumulate
        prog = progress(b, total=2*len(vals))
        for total in accumulate(prog):
            prog.set_postfix_str(f'Rows loaded: {total: 12,d}')
        result = prog.result()

    print(len(result), sum(result))
