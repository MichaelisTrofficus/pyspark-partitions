from setuptools import setup, find_packages

setup(
    name="nairobi",
    version="0.2",
    description="Nairobi Library",
    long_description="Nairobi Library",
    author="NBO MLE Team",
    author_email="",
    license="All rights reserved",
    test_suite="tests",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    keywords=["nairobi"],
)
