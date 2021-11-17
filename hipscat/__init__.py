#!/usr/bin/env python
# coding: utf-8

import healpy as hp
import pandas as pd
import os, os.path
from tqdm import tqdm
import glob

def pix2subdir(k, pix):
    # return a healpix subdirectory for the given order and pixel number
    # FIXME: turn this into a proper docstring
    return f'Norder{k}/Npix{pix}'

########################

def _to_hips(df, hipsPath, outFn='catalog', format='csv'):
    # WARNING: only to be used from df2hips(); it's defined out here just for debugging
    # convenience.

    # grab the order and pix number for this dataframe. Since this function
    # is intented to be called with apply() after running groupby on (k, pix), these must
    # be the same throughout the entire dataframe
    k, pix = df['hips_k'].iloc[0],  df['hips_pix'].iloc[0]
    assert (df['hips_k']   ==   k).all()
    assert (df['hips_pix'] == pix).all()

    # construct the output directory and filename
    dir = os.path.join(hipsPath, pix2subdir(k, pix))
    fn  = os.path.join(dir, f"{outFn}.{format}")

    # create dir if it doesn't exist
    os.makedirs(dir, exist_ok=True)

    if format == 'csv':
        # write to the file (append if it already exists)
        # also, write the header only if the file doesn't already exist
        df.to_csv(fn, mode='a', index=False, header=not os.path.exists(fn))
    elif format == 'pkl':
        # note: storing to pickle doesn't support appending
        assert not os.path.exists(fn)
        df.to_pickle(fn)

    # return the number of records written
    return len(df)

def df2hips(hipsPath, df, k, outFn='catalog.csv', format='csv'):
    # Dataframe-to-hips: write out a dataframe as a HiPS file of a given order
    # FIXME: rewrite this implementation so it doesn't modify the input dataframe

    # FIXME: write a proper docstring
    df['hips_k'] = k
    df['hips_pix'] = hp.ang2pix(2**k, df['ra'].values, df['dec'].values, lonlat=True)

    # returns the number of records written to each HiPS file
    return df.groupby(['hips_k', 'hips_pix']).apply(_to_hips, hipsPath=hipsPath, outFn=outFn, format=format)


########################

import dask

@dask.delayed
def _partition_inputs(url, hipsPath, k):
    # A function loads a file from an URL and writes out a series of
    # files into hipsPath named 'import.<url_basename>'. This guarantees
    # no other instance of _partition_worker will try to write to the same
    # file. These filles will be merged by _merge_worker() in a separate
    # pass.
    df = pd.read_csv(url)

    # write the file into 'catalog-Gaia...csv.gz' named files
    outFn = 'import.' + os.path.basename(url)
    ret = df2hips(hipsPath, df, k=k, outFn=outFn, format='pkl')
    return ret

@dask.delayed
def _compactify_partitions(idx, hipsPath, outFn, format):
    # for a given idx=(k, pix), list all 'import.*' files and merge them
    # into outFn (usually catalog.csv). Delete the import.* files once
    # the merge is done.
    k, pix = idx
    dir = os.path.join(hipsPath, pix2subdir(k, pix))
    destFn = os.path.join(dir, f'{outFn}.{format}')

    # load and merge temp files
    files = glob.glob(os.path.join(dir, 'import.*.pkl'))
    df = pd.concat( pd.read_pickle(fn) for fn in files )

    # write out in the desired format
    if format == 'csv':
        df.to_csv(destFn, mode='a', index=False, header=not os.path.exists(destFn))
    elif format == 'h5':
        df.to_hdf(destFn, mode='a', key='catalog', append=True, format='table', complib='blosc:lz4', complevel=9)
    elif format == 'parquet':
        df.to_parquet(destFn)
    else:
        raise Exception(f"Unknown format '{format}'")

    # delete temp files
    for fn in files:
        os.unlink(fn)


def csv2hips(hipsPath, urls, k=6, format='csv'):
    """
    Convert a list of URLs to CSV files (may be local files) to a HiPS file
    of a given order k.
    
    :param hipsPath: the output HiPS directory
    :param urls: a list of URLs with GAIA .csv files
    :param k: Healpix order

    :return: Number of records written to each HiPS file
    """
    from functools import reduce
    
    from dask.distributed import Client, progress, as_completed
    c = Client(n_workers=12, threads_per_worker=1, memory_limit='16GB')
    print(c)

    #
    # Stage #1: Import files in parallel, each into its own leaf .csv file
    #
    stage1 = c.compute([ _partition_inputs(url, hipsPath=hipsPath, k=k) for url in urls ])
    prog = tqdm(as_completed(stage1, with_results=True), total=len(urls))
    summary = None
    for _, df in prog:
            # FIXME: this changes the datatype to float... grrr...
            summary = summary.add(df, fill_value=0).astype(int) if summary is not None else df

            # give us the number of rows imported
            prog.set_postfix({'rows imported': int(summary.sum())})

    #
    # Stage #2: Merge the individual leaf files into a single file per partition
    #
    stage2 = c.compute([ _compactify_partitions(idx, hipsPath=hipsPath, outFn='catalog', format=format) for idx in summary.index ])
    for _ in tqdm(as_completed(stage2), total=len(summary)):
        pass

    return summary

def main():
    # HACK: for quick tests, run python -m hipscat
#    import glob
#    urls = glob.glob('./gdr2/*.csv.gz')

    from .util import get_gaia_csv_urls
    urls = get_gaia_csv_urls()
    print(f"Number of input files {len(urls)}")
##    import random
##    random.shuffle(urls)
    urls = urls[:25000]

    summary = csv2hips('output', urls, format='parquet')
    print(summary)
    print(f'Total rows imported: {summary.sum()}')
