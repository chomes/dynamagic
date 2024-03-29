
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dynamagic", # Replace with your own username
    version="1.4",
    author="chomes",
    author_email="jaayjay@gmail.com",
    description="Python client to interfact with dynamodb",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chomes/dynamagic",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)
