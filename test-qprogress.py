import dask.bag as db
from dask.distributed import Client, Queue
import numpy as np
from qprogress import progress, compute_with_progress

def some_fun(v, progress):
    progress.put(int(v))
    return v

if __name__ == "__main__":
    client = Client(n_workers=20, threads_per_worker=1)

    import test
    vals = np.arange(3000)

    q1, q2 = Queue(), Queue()
    b = db.from_sequence(vals).map(some_fun, progress=q1).map(lambda x: [x, 2*x]).flatten().map(some_fun, progress=q2)

    if True:
        prog = progress(b, q1, q2, total1=len(vals), total2=2*len(vals))
        totals = { q.name: 0 for q in [q1, q2] }
        for v in prog:
            totals[prog.q.name] += v
            prog.set_postfix_str(f'Rows loaded: {totals[prog.q.name]: 12,d}')
        result = prog.result()

    if False:
        result = compute_with_progress(b, q1, q2, desc1=" First map", desc2="Second map", total1=len(vals), total2=2*len(vals))

    if False:
        from itertools import accumulate
        prog = progress(b, q2, total=2*len(vals))
        for total in accumulate(prog):
            prog.set_postfix_str(f'Rows loaded: {total: 12,d}')
        result = prog.result()

    print(len(result), sum(result))
