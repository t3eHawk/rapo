"""Setup script for RAPO package."""

import rapo
import setuptools


name = rapo.__name__
version = rapo.__version__
author = rapo.__author__
author_email = rapo.__email__
description = rapo.__doc__
long_description = open('README.md', 'r').read()
long_description_content_type = 'text/markdown'
license = rapo.__license__
url = f'https://github.com/t3eHawk/{name}'
python_requires = '>=3.7'
install_requires = open('requirements.txt').read().splitlines()
packages = setuptools.find_packages()
package_data = {
    'rapo': [
        'web/api/templates/*',
        'web/api/ui/**/*',
        'algorithms/**/*'
    ]
}
classifiers = [
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.11',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent'
]


setuptools.setup(name=name,
                 version=version,
                 author=author,
                 author_email=author_email,
                 description=description,
                 long_description=long_description,
                 long_description_content_type=long_description_content_type,
                 license=license,
                 url=url,
                 python_requires=python_requires,
                 install_requires=install_requires,
                 packages=packages,
                 package_data=package_data,
                 classifiers=classifiers)
