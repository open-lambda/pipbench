#!/usr/bin/env python
import os, sys, json, subprocess, threading, time, random, csv
from collections import defaultdict as ddict
import gzip
import pkg_lookup

def human_bytes(b):
    b = float(b)
    if b < 1024:
        return '%.2f' % (b) + 'B'
    if b < 1024**2:
        return '%.2f' % (b/1024) + 'KB'
    if b < 1024**3:
        return '%.2f' % (b/(1024**2)) + 'MB'
    if b < 1024**4:
        return '%.2f' % (b/(1024**3)) + 'GB'
    return '%.2f' % (b/(1024**4)) + 'TB'

def perf_stats():
    stats = ddict(list)
    with open('./import.json') as f:
        for l in f:
            row = json.loads(l)
            if row['stage'] != 'pass2':
                continue
            stats['import_cpu'].append(row['import_ms'] / 1000.0) # ms => sec
            stats['import_mem'].append(row['mem']) # ms => sec
    with open('./install.json') as f:
        for l in f:
            row = json.loads(l)
            stats['install_cpu'].append(row['install_ms'] / 1000.0) # ms => sec

    return stats

def main():
    new_count = int(sys.argv[1])
    print 'create %d new nodes' % new_count

    print 'grab perf sample'
    perf_sample = perf_stats()
    
    # fetch orig data
    print 'load orig graph'
    with gzip.open('levels.json.gz') as f:
        orig_levels = json.load(f)
        
    print 'got %d orig levels' % len(orig_levels)
    orig_count = sum(map(len, orig_levels))
    print 'got %d orig nodes' % orig_count
    ratio = float(new_count) / orig_count
    print 'use resize ratio %.2f' % ratio

    # gen new nodes
    new_levels = []
    popularity_tickets = [] # (m,n): n'th ticket from m'th level
    for level_num, orig_level in enumerate(orig_levels):
        new_levels.append([])
        popularity_tickets.append([])
        
        level_size = max(int(len(orig_level) * ratio), 1) # min 1
        print 'resize %d old nodes at level %d to %d new nodes' % (len(orig_level), level_num, level_size)

        for i in range(level_size):
            orig_node = random.choice(orig_level)
            name = 'level%dpkg%d' % (level_num, i)
            
            # edge properties
            new_node = {'name': name, 'popularity': orig_node['in'], 'deps': list(orig_node['out'])}

            # size properties
            for key in ['uncompressed', 'compressed', 'subfiles']:
                new_node[key] = orig_node[key]

            # perf properties
            new_node['install_cpu'] = random.choice(perf_sample['install_cpu'])
            new_node['import_cpu'] = random.choice(perf_sample['import_cpu'])
            new_node['import_mem'] = random.choice(perf_sample['import_mem'])
            new_levels[level_num].append(new_node)

            # chance of being picked by higher level is proportionate to popularity
            for j in range(new_node['popularity']):
                popularity_tickets[level_num].append(new_node)

        # at least one ticket at each level
        if len(popularity_tickets[level_num]) == 0:
            popularity_tickets[level_num].append(random.choice(new_levels[level_num]))

    # concat all levels
    print 'concat all nodes'
    new_nodes = []
    for level in new_levels:
        new_nodes.extend(level)

    # finish resolving edge targets
    print 'resolve edge targets'
    for node in new_nodes:
        for i in range(len(node['deps'])):
            target_level = node['deps'][i]
            winner = random.choice(popularity_tickets[target_level])
            node['deps'][i] = winner['name']
        # dedup deps
        node['deps'] = list(set(node['deps']))

    # dump
    path = 'new-graph-%d.json' % new_count
    print 'dump to ' + path
    with open(path, 'w') as f:
        json.dump(new_nodes, f, indent=True)    # dump

    # summary
    print 'repo stats:'
    compressed = sum([n['compressed'] for n in new_nodes])
    uncompressed = sum([n['uncompressed'] for n in new_nodes])
    print 'Total Compressed Size: ' + human_bytes(compressed)
    print 'Total Uncompressed Size: ' + human_bytes(uncompressed)
            

    
if __name__ == '__main__':
    main()
