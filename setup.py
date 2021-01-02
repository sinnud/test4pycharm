import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="postgresql-sinnud", # Replace with your own username
    version="0.0.2",
    author="Luke Du",
    author_email="sinnud@gmail.com",
    description="A package for connection and utilities to PostgreSQL database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sinnud/test4pycharm",
    packages=setuptools.find_packages(),
    install_requires = [
        'logzero',
        'psycopg2',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)