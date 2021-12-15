from instastart.auto import start, done

import dask.distributed as dd
#import astropy
#import vaex
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
    start()

    print("Here!")
#    tqdm_echo()
#    os.execl('/usr/bin/htop', 'htop')

    done()
    pass
#        os.execl("/astro/users/mjuric/lfs/bin/joe", "joe")
#        print(f"{__file__=}")
#        print("I'm here!")
