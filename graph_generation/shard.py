#!/usr/bin/env python
import os, sys, json, subprocess, threading, time
from collections import defaultdict as ddict
import gzip

def get_skip():
    skip = set()
    for path in os.listdir('out'):
        path = 'out/'+path
        with open(path) as f:
            for l in f:
                try:
                    row = json.loads(l)
                except:
                    pass
                if row['error'] == '[Errno 28] No space left on device':
                    continue
                skip.add(row['from'])
    return skip

def reshard(shards, limit=None):
    skip = get_skip()

    files = []
    for i in range(shards):
        files.append(open('todo/%d.txt'%i, 'w'))

    idx = 0
    with gzip.open('../1/pkg-index.json.gz', 'r') as f:
        for pkg_num,l in enumerate(f):
            pkg = json.loads(l)
            for version_num,version in enumerate(pkg['versions']):
                if (version['archive'] in skip):
                    continue
                files[idx%len(files)].write(version['archive'] + '\n')
                idx += 1

            if limit != None and idx > limit:
                break
                
    for f in files:
        f.close()

    return idx

def main():
    limit = None
    if len(sys.argv) > 2:
        limit = int(sys.argv[2])
    count = reshard(int(sys.argv[1]), limit)
    print '%d tasks' % count
    
if __name__ == '__main__':
    main()
