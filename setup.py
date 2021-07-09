from setuptools import setup, find_packages

requirements = [
    "dweepy",
    "random_name",
    "easydict",
    "kafka-python",
    "concurrent_log_handler",
    "python-json-logger",
]
setup(
    name="decentlog",
    scripts=["scripts/decent_cli.py"],
    version="0.1.0",
    description="Decentralized logging, log anywhere and view in your console",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Sandip Dey",
    author_email="sandip.dey1988@yahoo.com",
    url="https://github.com/sandyz1000/decentlog",
    license="MIT License",
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    platforms=["linux", "unix"],
    python_requires=">3.5.2",
)
