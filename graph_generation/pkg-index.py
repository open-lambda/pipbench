#!/usr/bin/env python
import os, sys, json
from collections import defaultdict as ddict
from bs4 import BeautifulSoup

MIRROR = '/mnt/mirror'

def versions(pkg):
    prefix = '../../'
    rv = []
    with open(MIRROR + '/web/simple/%s/index.html' % pkg) as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
        for link in soup.find_all('a'):
            path = link.get('href')
            assert(path.startswith(prefix))
            path = path[len(prefix):].split('#md5=')[0]
            rv.append({'name':link.text, 'archive': path})
    return rv

def main():
    with open(MIRROR + '/web/simple/index.html') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    with open('pkg-index.json', 'w') as f:
        for link in soup.find_all('a'):
            assert(link.text + '/' == link.get('href'))
            pkg = link.text
            f.write(json.dumps({'name':pkg, 'versions':versions(pkg)}) + '\n')

if __name__ == '__main__':
    main()
