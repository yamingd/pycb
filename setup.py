try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


requires = [
    'pylcb'
]


setup(
    name='couchbase',
    description='Tower3 couchbase library.',
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
    package_dir={'couchbase': 'couchbase'},
    packages=[
        'couchbase',
    ],
    install_requires=requires
)
