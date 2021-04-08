from setuptools import setup, find_packages

setup(
    name='tunetools',
    version='0.2.0',
    description='A simple and powerful tune tool for AI experiments',
    author='Keytoize',
    author_email='cmx_1007@foxmail.com',
    packages=find_packages(where='.', exclude=(), include=('*',)),
    entry_points={
        'console_scripts': [
            'tunetools = tunetools.main:main'
        ]
    }
)