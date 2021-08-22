from setuptools import setup, find_packages

exec(open('tunetools/package_info.py').read())

setup(
    name='tunetools',
    version=__version__,
    description='A powerful tuning and result management toolkit for AI experiments',
    author=__author__,
    author_email='cmx_1007@foxmail.com',
    packages=find_packages(where='.', exclude=(), include=('*',)),
    entry_points={
        'console_scripts': [
            'tunetools = tunetools.main:main'
        ]
    },
    install_requires=[
        'pandas',
        'scipy',
        'PyYaml'
    ]
)