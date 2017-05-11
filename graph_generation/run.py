#!/usr/bin/python
import os, sys, time, json, subprocess

MIRROR = '/mnt/mirror/web/'

def cmd(c):
    print 'RUN: ' + c
    out = subprocess.check_output(c, shell=True)
    print out
    return out

def todo_iter():
    with open('top.json') as f:
        for l in f:
            todo = json.loads(l)
            yield todo

def install():
    for todo in todo_iter():
        cmd('pip install -i file://'+MIRROR+'simple ' + todo['package'])

def import_perf():
    for todo in todo_iter():
        cmd('python measure-mod.py %s pass1 >> import.json' % todo['module'])
        cmd('python measure-mod.py %s pass2 >> import.json' % todo['module'])

def install_perf():
    f = open('install.json', 'w')
    for todo in todo_iter():
        cmd('rm -f ./download/*')
        cmd('rm -rf ./install/*')
        cmd('pip download --no-deps -d ./download -i file://'+MIRROR+'simple ' + todo['package'])
        files = list(os.listdir('./download'))
        assert(len(files) == 1)
        path = './download/'+files[0]
        print path

        time1 = time.time()
        cmd('pip install --no-deps %s -t ./install' % path)
        time2 = time.time()
        row = {}
        row.update(todo)
        row['install_ms'] = round((time2-time1)*1000.0,3)
        f.write(json.dumps(row) + '\n')
    f.close()
        
def main():
    command = sys.argv[1]
    if command == 'install':
        install()
    if command == 'import_perf':
        import_perf()
    if command == 'install_perf':
        install_perf()

if __name__ == '__main__':
    main()



#cmd = 'docker run -d pip-check pip install --no-deps "%s"' % path

#, args = reqs.parse_args([path, '-i', 'file://'+MIRROR+'simple'])
