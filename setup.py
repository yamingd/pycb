import os
import sys
from distutils.core import setup, Extension


pylcb_ext = Extension(
    'pylcb', ['src/pylcb/pylcb.c'],
    include_dirs=[
        '/usr/local/include/libcouchbase',
        '/usr/include/libcouchbase'
    ],
    libraries=['couchbase'],
)


if sys.platform == 'darwin' and not os.environ.get('ARCHFLAGS'):
    os.environ['ARCHFLAGS'] = '-arch x86_64'


setup(
    name='pycb',
    description='Tower3 couchbase library.',
    long_description=(
        '%s\n\n%s' % (
            open('README.md').read(),
            open('CHANGELOG.md').read()
        )
    ),
    version=open('VERSION').read().strip(),
    author='Tower3',
    author_email='pycb@tower3.io',
    url='http://tower3.io',
    license=open('LICENSE').read(),
    ext_modules=[pylcb_ext],
    package_dir={'': 'src'},
    packages=[
        'pycb',
    ]
)
