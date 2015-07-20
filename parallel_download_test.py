import os
import subprocess
from jobTree.target import Target
import multiprocessing


def download_file(pair):
    url, name = pair
    subprocess.check_call(['curl', '-fs', '--create-dir', url, '-o', name])


def start(target, *args):
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    names = [str(i) + '.file' for i in xrange(len(args))]
    pool.map(download_file, zip(args, names))
    target.addChildTargetFn(stuff, names)


def stuff(target, names):
    for name in names:
        os.remove(name)


if __name__ == '__main__':
    options = Target.Runner.getDefaultOptions()
    Target.Runner.startJobTree(Target.wrapTargetFn(start, 'www.google.com', 'www.google.com', 'www.google.com'), options)
    Target.Runner.cleanup(options)