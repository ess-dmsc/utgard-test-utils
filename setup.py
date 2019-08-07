import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="utgardtests",
    version="0.1.0",
    author="Afonso Mukai",
    author_email="afonso.mukai@esss.se",
    description="Utilities for test automation at Utgård",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ess-dmsc/utgard-test-utils",
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "confluent-kafka==1.1.0",
        "fabric==2.4.0",
        "h5py==2.9.0",
        "matplotlib==3.1.0",
        "numpy==1.16.4",
        "pandas==0.24.2",
        "requests==2.22.0",
        "scikit-learn==0.21.2",
        "scipy==1.3.0",
        "statsmodels==0.10.0",
        "tables==3.5.2",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
)
