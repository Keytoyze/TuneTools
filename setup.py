from setuptools import setup, find_packages

exec(open('tunetools/package_info.py').read())

setup(
    name='tunetools',
    version=__version__,
    description='A powerful tuning and result management toolkit for AI experiments',
    long_description_content_type='text/x-rst',
    long_description='Documentation: https://github.com/Keytoyze/TuneTools/blob/main/README.md',
    author=__author__,
    author_email='cmx_1007@foxmail.com',
    url='https://github.com/Keytoyze/TuneTools',
    packages=find_packages(where='.', exclude=(), include=('*',)),
    entry_points={
        'console_scripts': [
            'tunetools = tunetools.main:main'
        ]
    },
    install_requires=[
        'numpy',
        'pandas',
        'scipy>=1.6.0',
        'PyYaml'
    ]
)