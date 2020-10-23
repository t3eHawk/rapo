"""Setup script for RAPO package."""

import rapo
import setuptools


name = 'rapo'
version = rapo.__version__
author = rapo.__author__
author_email = rapo.__email__
description = rapo.__doc__
long_description = open('README.md', 'r').read()
long_description_content_type = 'text/markdown'
license = rapo.__license__
url = 'https://github.com/t3eHawk/rapo'
install_requires = ['pepperoni', 'psutil',
                    'sqlalchemy', 'cx_oracle', 'sqlparse',
                    'flask', 'flask_httpauth', 'waitress']
packages = setuptools.find_packages()
classifiers = ['Programming Language :: Python :: 3',
               'License :: OSI Approved :: MIT License',
               'Operating System :: OS Independent']


setuptools.setup(name=name,
                 version=version,
                 author=author,
                 author_email=author_email,
                 description=description,
                 long_description=long_description,
                 long_description_content_type=long_description_content_type,
                 license=license,
                 url=url,
                 install_requires=install_requires,
                 packages=packages,
                 classifiers=classifiers)
