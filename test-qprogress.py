import dask.bag as db
from dask.distributed import Client, Queue
import numpy as np
from qprogress import progress, compute_with_progress, log

def some_fun(v):
    log('prog1', int(v))
    return v

def some_fun2(v):
    log('prog2', int(v))
    return v

if __name__ == "__main__":
    client = Client(n_workers=3, threads_per_worker=1)

    import test
    vals = np.arange(2000)

    b = db.from_sequence(vals).map(some_fun).map(lambda x: [x, 2*x]).flatten().map(some_fun2)

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
