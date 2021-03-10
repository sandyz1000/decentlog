from setuptools import setup, find_packages

setup(
    name="decentlog",
    scripts=["decentlog/decent_cli"],
    version="0.1.0",
    description="Decentralized logging using dweet.io !",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Sandip Dey",
    author_email="sandip.dey1988@yahoo.com",
    url="https://github.com/sandyz1000/decentlog",
    license="MIT License",
    packages=find_packages(),
    include_package_data=True,
    install_requires=["dweepy", "random_name"],
    platforms=["linux", "unix"],
    python_requires=">3.5.2",
)
