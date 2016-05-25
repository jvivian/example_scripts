import os
import subprocess
import textwrap


def setup():
    """Calls setup script which installs pipeline dependencies"""
    subprocess.check_call(['/usr/bin/bash', '-ex'], stdin=textwrap.dedent("""
        virtualenv s3am
        . s3am/bin/activate
        pip install --pre s3am
        deactivate
        # Create Toil venv
        virtualenv toil
        . venv/bin/activate
        pip install pytest toil boto
        deactivate
        # Expose binaries to the PATH
        mkdir bin
        export PATH=$PATH:${PWD}/bin
        ln -snf ${PWD}/s3am/bin/s3am bin/
        ln -snf ${PWD}/toil/bin/toil bin/
        ln -snf ${PWD}/toil/bin/boto bin/
        ln -snf ${PWD}/toil/bin/pytest bin/
        # Set PYTHONPATH
        export PYTHONPATH=$(python -c 'from os.path import abspath as a;import sys;print a("src")' $0)"""))


def determine_modified_subdirs():
    """Determines directories modified since previous commit"""
    out = subprocess.check_output(['git', 'diff-tree', '--name-only', 'HEAD', '-r']).split('\n')
    return {os.path.dirname(x)for x in out if 'src' in x}


def contains_test(path):
    """If directory contains a 'test' subdir, returns non-empty set"""
    return {x for x in next(os.walk(path))[1] if 'test' in x}


def main():
    setup()
    modified_subdirs = determine_modified_subdirs()
    subdirs_to_test = filter(contains_test, modified_subdirs)
    for i, subdir in enumerate(subdirs_to_test):
        subprocess.check_call('py.test', subdir, '--doctest-modules', '--junitxml=test-report-{}.xml'.format(i))


if __name__ == '__main__':
    main()
