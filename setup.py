import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dolosse",
    version="0.0.1",
    author="S. V. Paulauskas and the Dolosse Collaboration",
    author_email="stan@projectscience.tech",
    description="A Kafka based data acquisition and analysis framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dolosse/dolosse",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Scientific/Engineering :: Physics",
        "Operating System :: OS Independent",
    ],
)