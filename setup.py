from setuptools import setup, find_packages

setup(
    name='cc_dynamodb',
    packages=find_packages(),
    dependency_links=['git+https://github.com/virtuald/pyhcl.git@0830b300774f94b930255bded91c08cb03c1df8e#egg=pyhcl'],
    install_requires=[
        'bunch>=1.0.1',
        'boto>=2.31.1',
        'PyYAML==3.10',
        'pyhcl',
    ],
    tests_require=['pytest', 'mock', 'factory_boy'],
    version='1.0.2',
    description='A dynamodb common configuration abstraction',
    author='Paul Craciunoiu',
    author_email='pcraciunoiu@clearcareonline.com',
    url='https://github.com/clearcare/cc_dynamodb',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: OS Independent',
        'Development Status :: 1 - Planning',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Topic :: System :: Distributed Computing',
    ]
)
