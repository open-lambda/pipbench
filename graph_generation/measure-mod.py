#!/usr/bin/python
from guppy import hpy
from importlib import import_module
import sys, time, json

def chop_path(path):
    prefixes = sorted(sys.path, key=lambda path: -len(path))
    for prefix in prefixes:
        if path.startswith(prefix):
            return path[len(prefix):]

def log(m):
    sys.stderr.write(m + '\n')
        
def main():
    h = hpy()

    target, stage = sys.argv[1:]
    assert(stage in ['pass1', 'pass2', 'sanity'])

    # load indirect costs first
    if stage in ['pass2', 'sanity']:
        with open('pass1.mods') as f:
            for mod in f:
                mod = mod.strip()
                if mod.split('.')[0] != target or stage == 'sanity':
                    log('<LOAD> ' + mod)
                    import_module(mod)
                else:
                    log('<SKIP> ' + mod)
                    
    # measure memory cost of loading module
    mods1 = dict(sys.modules)
    size1 = h.heap().size
    time1 = time.time()
    import_module(target)
    time2 = time.time()
    size2 = h.heap().size
    mods2 = dict(sys.modules)

    if stage == 'pass1':
        new_mods = set(mods2.values()) - set(mods1.values())
        mod_names = []
        for mod in new_mods:
            path = getattr(mod, '__file__', None)
            if path != None:
                path = chop_path(path)
                parts = path.split('/')
                if parts[-1].startswith('__init__.py'):
                    parts = filter(lambda p: p != '', parts)
                    mod_names.append('.'.join(parts[:-1]))
                    
        with open('pass1.mods', 'w') as f:
            for m in mod_names:
                f.write(m+'\n')

    size = size2 - size1
    log('%.3f MB' % (size / (1024.0**2)))
    log('%.3f seconds' % (time2 - time1))

    row = {'mem': size, 'import_ms': round((time2-time1)*1000.0,3), 'stage': stage, 'module': target}
    print json.dumps(row)
    
if __name__ == '__main__':
    main()
