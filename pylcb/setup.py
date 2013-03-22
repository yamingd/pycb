from distutils.core import setup, Extension

pylcb = Extension(
    'pylcb',
    include_dirs=['/usr/local/Cellar/libcouchbase/2.0.4/include'],
    libraries=['couchbase'],
    sources=['pylcb.c']
)

setup(
    name='pylcb',
    description='Tower3 libcouchbase C extension',
    long_description=(
        '%s\n\n%s' % (
            open('README.md').read(),
            open('CHANGELOG.md').read()
        )
    ),
    version=open('VERSION').read().strip(),
    author='Tower3',
    author_email='devops@tower3.io',
    url='http://tower3.io',
    license=open('LICENSE').read(),
    ext_modules=[pylcb]
)
