import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="jsonutils",
    version="1.0.0",
    author="Luke Du",
    author_email="sinnud@gmail.com",
    description="JSON utils",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sinnud/test4pycharm.git@jsonutils",
    packages=setuptools.find_packages(),
    install_requires = [],
    classifiers=[
    ],
    python_requires='>=3.6',
    include_package_data=True
)
