#!/usr/bin/env python
import os, sys, json, subprocess, threading, time, subprocess
from collections import defaultdict as ddict
import gzip
import shard

def used_space(path):
    out = subprocess.check_output('df /mnt/ramdisk', shell=True)
    lines = [line.split() for line in out.split('\n')]
    return int(lines[1][lines[0].index('Use%')].rstrip('%'))

def main():
    shards = int(sys.argv[1])
    
    while True:
        tasks = shard.reshard(shards)
        print '%d tasks in next batch' % tasks
        if tasks == 0:
            print 'all done'
            return
    
        container_ids = []

        print 'start shards'
        for i in range(shards):
            inpath = '/todo/%d.txt' % i
            j = 0
            while True:
                outpath = '/out/%d.%d.json' % (i, j)
                if not os.path.exists('.'+outpath):
                    break
                j += 1
            cmd = ('docker run -d -w /var/edges ' +
                   '-v /mnt/mirror:/mnt/mirror:ro '+
                   '-v `pwd`/todo:/todo:ro ' +
                   '-v `pwd`/out:/out pip-edges ' +
                   'python check.py %s %s' % (inpath, outpath))
            print cmd
            cid = subprocess.check_output(cmd, shell=True)
            cid = cid.strip()
            print 'got container id '+cid
            container_ids.append(cid)

        # wait up to 20 minutes
        for i in range(20*60):
            time.sleep(1)
            if used_space('/mnt/ramdisk') > 75:
                break # restart early
            
        print 'cleanup'
        for cid in container_ids:
            cmd = 'docker rm -f ' + cid
            print 'RUN: ' + cmd
            rc = os.system(cmd)
            print 'RESULT:', rc

if __name__ == '__main__':
    main()
