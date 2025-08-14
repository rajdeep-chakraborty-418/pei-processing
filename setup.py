"""
Wheel Preparation for the Module
"""
from setuptools import (
    setup,
    find_packages,
)

setup(
    name='pei_processor',
    version='0.0.1',
    long_description=open("README.md", "r").read(),
    long_description_content_type='text/markdown',
    package_dir={'': '.'},
    include_package_data=False,
    packages=find_packages(
        where='.',
        include=['*.*'],
        exclude=['*tst*']
    ),
    entry_points={
        'console_scripts': [
            'run-flow = src.main.flow:main'
        ]
    },
    url='',
    author='rajdeep-chakraborty-418',
    author_email='rajdeepchakraborty728@gmail.com',
    description='pei_processor_wheel',
    extras_require={'dev': ['pytest']},
    python_requires='>=3.10',
    setup_requires=['pytest-runner', 'wheel'],
    test_suite="test"
)
