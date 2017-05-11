#!/usr/bin/env python
import os, sys, json, csv, subprocess, threading, time
from collections import defaultdict as ddict
import gzip
import pkg_lookup

class Node:
    def __init__(self, name):
        self.name = name
        self.incoming = []
        self.outgoing = []
        self.height = None
        self.uncompressed = None
        self.compressed = None
        self.subfiles = None

    def __str__(self):
        full = pkg_lookup.lookup(self.name)
        return str(full)

class Graph:
    def __init__(self):
        self.nodes = dict()
        self.size_stats = load_size_stats()

    def get(self, name):
        node = self.nodes.get(name, None)
        if node == None:
            node = Node(name)
            node_size_stats = self.size_stats[name]
            if node_size_stats['uncompressed'] == '':
                return None # don't add a node if we couldn't peek inside it
            node.uncompressed = int(node_size_stats['uncompressed'])
            node.compressed = int(node_size_stats['compressed'])
            node.subfiles = int(node_size_stats['subfiles'])
            self.nodes[name] = node
        return node

    def count_incoming(self):
        return sum([len(node.incoming) for node in self.nodes.values()])
    
    def count_outgoing(self):
        return sum([len(node.outgoing) for node in self.nodes.values()])
    
def dup_check():
    line_num = dict()
    with gzip.open('../3/edges.json.gz', 'r') as f:
        for i, l in enumerate(f):
            row = json.loads(l)
            source_id = (row['from']['pkg_num'], row['from']['version_num'])
            if source_id in line_num:
                print 'Dup: lines %d and %d' % (line_num[source_id], i+1)
            line_num[source_id] = i

def load_size_stats():
    stats = {}
    with gzip.open('../5/pkg-sizes.csv.gz') as f:
        reader = csv.DictReader(f)
        for row in reader:
            stats[(int(row['pkg_num']), int(row['version_num']))] = row

    return stats
    
def load_graph(limit=None):
    graph = Graph()
    
    with gzip.open('../3/edges.json.gz', 'r') as f:
        for i, l in enumerate(f):
            if limit != None and i > limit:
                break
            if i % 1000 == 0:
                print '%d nodes loaded' % i
               
            row = json.loads(l)
            source_id = (row['from']['pkg_num'], row['from']['version_num'])          
            source = graph.get(source_id)
            if source == None:
                continue
            
            for target in row['to']:
                if target == None:
                    continue # TODO: shouldn't happen once we get good data
                target_id = (target['pkg_num'], target['version_num'])
                target = graph.get(target_id)
                if target == None:
                    continue
                source.outgoing.append(target)
                target.incoming.append(source)
    return graph

# can we reach goal node from curr node?  Let's recurse to find out!
def is_reachable(goal, curr, visited=set()):
    # avoid getting stuck
    if curr in visited:
        return
    visited.add(curr)
    
    for child in curr.outgoing:
        # two ways to be part of a loop: directly or indirectly...
        if child == goal:
            print 'GOT TO GOAL!'
            print str(goal)
            print str(curr)
            return True
        if is_reachable(goal, child, visited):
            print str(curr)
            return True
        return False

def main():
    print 'get graph'
    graph = load_graph()

    print 'incoming: %d' % graph.count_incoming()
    print 'outgoing: %d' % graph.count_outgoing()

    print 'find heights'
    nodes = list(graph.nodes.values())
    levels = []

    height = 0
    while len(nodes) > 0:
        print 'process height %d' % height
        print '%d nodes remaining' % len(nodes)
        levels.append([])
        next_nodes = [] # work to do in next iteration
        for node in nodes:
            is_at_height = True
            for child in node.outgoing:
                if child.height == None or child.height >= height:
                    is_at_height = False
                    break
            if is_at_height:
                node.height = height
                levels[height].append(node)
            else:
                next_nodes.append(node)

        # check for loops
        if len(nodes) == len(next_nodes) and len(next_nodes) > 0:
            print 'the remaining %d packages cause dep loops, so skip them' % len(next_nodes)
            break
                
        nodes = next_nodes
        height += 1

    # dump
    print 'dump levels'
    dump = []
    for level in levels:
        if len(level) == 0:
            break
        nodes = []
        for node in level:
            nodes.append({'in':len(node.incoming), 'out': [dep.height for dep in node.outgoing],
                          'uncompressed': node.uncompressed,
                          'compressed': node.compressed,
                          'subfiles': node.subfiles})
            assert(node.uncompressed != None)
            assert(node.compressed != None)
            assert(node.subfiles != None)
        dump.append(nodes)

    print 'after pruning to make a DAG:'
    print 'total nodes dumped: %d' % sum(len(nodes) for nodes in dump)
    print 'total outgoing dumped: %d' % sum(sum([node['in'] for node in nodes]) for nodes in dump)
    print 'total incoming dumped: %d' % sum(sum([len(node['out']) for node in nodes]) for nodes in dump)
    
    with open('levels.json', 'w') as f:
        f.write(json.dumps(dump))
        f.write('\n')

    # compress
    print 'compress'
    rc = os.system('gzip -f levels.json')
    assert(rc == 0)
    
if __name__ == '__main__':
    main()
