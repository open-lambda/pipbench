#!/usr/bin/env python
import os, sys, json, csv, subprocess, threading, time
from collections import defaultdict as ddict
import gzip

def get_reverse_index():
    idx = dict()
    with gzip.open('./pkg-index.json.gz', 'r') as f:
        for pkg_num,l in enumerate(f):
            pkg = json.loads(l)
            for version_num,version in enumerate(pkg['versions']):
                idx[(pkg_num, version_num)] = version['archive']
    return idx

idx = None

def lookup(node_id):
    global idx
    if idx == None:
        print 'generating reverse index'
        idx = get_reverse_index()
        print 'done'
    return idx[node_id]
