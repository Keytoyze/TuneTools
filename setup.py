from setuptools import setup, find_packages

setup(
    name='tunetools',
    version='0.5.2',
    description='A powerful tuning and result management toolkit for AI experiments',
    author='Keytoize',
    author_email='cmx_1007@foxmail.com',
    packages=find_packages(where='.', exclude=(), include=('*',)),
    entry_points={
        'console_scripts': [
            'tunetools = tunetools.main:main'
        ]
    }
)