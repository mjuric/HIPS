import instastart.auto

import dask.distributed as dd
import os

def func():
    pass

import time, tqdm, sys
def tqdm_echo():
    for _ in tqdm.tqdm(range(20)):
        time.sleep(0.1)
    print("Out of the loop")
    return
    try:
        for line in sys.stdin:
            print("ECHO:", line, end='')
        pass
    except Exception:
        print("EXCEPTION", e)
        raise
    print("Exiting tqdm_echo")

if __name__ == "__main__":
    instastart.auto.start()

#    tqdm_echo()

    instastart.auto.done()
    pass
#        os.execl("/astro/users/mjuric/lfs/bin/joe", "joe")
#        print(f"{__file__=}")
#        print("I'm here!")
