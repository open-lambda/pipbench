#!/usr/bin/env python
import os, sys, json, gzip, re, zipfile, tarfile, multiprocessing
from collections import defaultdict as ddict

MIRROR = '/mnt/mirror/web'

def get_zip_stats(path):
    abspath = MIRROR + '/' + path
    with zipfile.ZipFile(abspath) as zf:
        infos = zf.infolist()
        return {'path': path, 'compressed': os.stat(abspath).st_size,
                'uncompressed': sum(map(lambda i: i.file_size, infos)), 'subfiles': len(infos)}

def get_tar_stats(path):
    abspath = MIRROR + '/' + path
    with tarfile.open(abspath) as tf:
        infos = filter(lambda info: info.isreg(), tf)
        return {'path': path, 'compressed': os.stat(abspath).st_size,
                'uncompressed': sum(map(lambda i: i.size, infos)), 'subfiles': len(infos)}

def get_other_stats(path):
    abspath = MIRROR + '/' + path
    return {'path': path, 'compressed': os.stat(abspath).st_size}

def work(path):
    try:
        if path.endswith('.zip') or path.endswith('.whl') or path.endswith('.egg'):
            return get_zip_stats(path)
        elif path.endswith('.tar.gz'):
            return get_tar_stats(path)
        else:
            return get_other_stats(path)
    except Exception as e:
        return {'path': path, 'error': str(e)}

def path_iter():
    with gzip.open('./pkg-index.json.gz', 'r') as f:
        for pkg_num,l in enumerate(f):
            pkg = json.loads(l)
            for version_num,version in enumerate(pkg['versions']):
                yield version['archive']
    
def main():
    pool = multiprocessing.Pool(40)

    print 'grab paths'
    paths = list(path_iter())

    f = open('sizes.json', 'w')
    
    chunk = 1000
    start = 0
    while start < len(paths):
        print 'index: %d\n' % start
        output = pool.map(work, paths[start:start+chunk])
        for row in output:
            if row != None:
                f.write(json.dumps(row) + '\n')
        start += chunk
        
    f.close()

    # WOSC stats:
    #tar.gz 539947
    #whl 129290
    #zip 66232
    #egg 53631

if __name__ == '__main__':
    main()
