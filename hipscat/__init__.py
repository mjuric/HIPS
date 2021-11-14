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

def _to_hips(df, hipsPath, outFn='catalog.csv'):
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
    fn  = os.path.join(dir, outFn)

    # create dir if it doesn't exist
    os.makedirs(dir, exist_ok=True)

    # write to the file (append if it already exists)
    # also, write the header only if the file doesn't already exist
    df.to_csv(fn, mode='a', index=False, header=not os.path.exists(fn))

    # return the number of records written
    return len(df)

def df2hips(hipsPath, df, k, outFn='catalog.csv'):
    # Dataframe-to-hips: write out a dataframe as a HiPS file of a given order
    # FIXME: rewrite this implementation so it doesn't modify the input dataframe

    # FIXME: write a proper docstring
    df['hips_k'] = k
    df['hips_pix'] = hp.ang2pix(2**k, df['ra'].values, df['dec'].values, lonlat=True)

    # returns the number of records written to each HiPS file
    return df.groupby(['hips_k', 'hips_pix']).apply(_to_hips, hipsPath=hipsPath, outFn=outFn)


########################

def _partition_worker(url, hipsPath, k):
    # A function loads a file from an URL and writes out a series of
    # files into hipsPath named 'import.<url_basename>'. This guarantees
    # no other instance of _partition_worker will try to write to the same
    # file. These filles will be merged by _merge_worker() in a separate
    # pass.
    df = pd.read_csv(url)

    # write the file into 'catalog-Gaia...csv.gz' named files
    outFn = 'import.' + os.path.basename(url)
    ret = df2hips(hipsPath, df, k=k, outFn=outFn)
    return ret

def _merge_worker(idx, hipsPath, outFn):
    # for a given idx=(k, pix), list all 'import.*' files and merge them
    # into outFn (usually catalog.csv). Delete the import.* files once
    # the merge is done.
    k, pix = idx
    dir = os.path.join(hipsPath, pix2subdir(k, pix))
    destFn = os.path.join(dir, outFn)

    for fn in glob.glob(os.path.join(dir, 'import.*')):
        df = pd.read_csv(fn)
        df.to_csv(destFn, mode='a', index=False, header=not os.path.exists(destFn))
        os.unlink(fn)

def csv2hips(hipsPath, urls, k=5):
    """
    Convert a list of URLs to CSV files (may be local files) to a HiPS file
    of a given order k.
    
    :param hipsPath: the output HiPS directory
    :param urls: a list of URLs with GAIA .csv files
    :param k: Healpix order

    :return: Number of records written to each HiPS file
    """
    from functools import partial
    from multiprocessing import Pool

    summary = None
    with Pool(12) as pool:
        #
        # Stage #1: Import files in parallel, each into its own leaf .csv file
        #
        worker = partial(_partition_worker, hipsPath=hipsPath, k=k)
        prog = tqdm(pool.imap_unordered(worker, urls), total=len(urls))
        for ret in prog:
            # Keep track of how many rows we've imported into
            # each individual HiPS file
            if summary is None:
                summary = ret
            else:
                # FIXME: this changes the datatype to float... grrr...
                summary = summary.add(ret, fill_value=0).astype(int)

            # progress update
            prog.set_postfix({'rows imported': summary.sum()})

        #
        # Stage #2: Merge the individual files into a single catalog.csv file
        #
        worker = partial(_merge_worker, hipsPath=hipsPath, outFn='catalog.csv')
        prog = tqdm(pool.imap_unordered(worker, summary.index), total=len(summary))
        for _ in prog:
            pass

    return summary

def main():
    # HACK: for quick tests, run python -m hipscat
#    import glob
#    urls = glob.glob('./gdr2/*.csv.gz')

    from .util import get_gaia_csv_urls
#    urls = get_gaia_csv_urls()[0:1000]
    urls = get_gaia_csv_urls()[1000:2000]

    summary = csv2hips('output', urls)
    print(summary)
    print(f'Total rows imported: {summary.sum()}')
