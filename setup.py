"""Setup of RAPO."""

import rapo
import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

install_requires = ['sqlalchemy', 'cx_oracle', 'pepperoni']

author = rapo.__author__
email = rapo.__email__
version = rapo.__version__
description = rapo.__doc__
license = rapo.__license__

setuptools.setup(
    name='rapo',
    version=version,
    author=author,
    author_email=email,
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',
    license=license,
    url='https://github.com/t3eHawk/rapo',
    install_requires=install_requires,
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
