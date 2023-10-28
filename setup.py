"""Setup script for the sales_forecast package."""

from setuptools import find_packages, setup


def load_required_dependencies() -> list[str]:
    """Load the required dependencies from the requirements.txt file.

    Returns:
        list[str]: List of required dependencies.
    """
    with open("requirements.txt") as f:
        required_dependencies = f.read().splitlines()
    return required_dependencies


setup(
    name="sales_forecast",
    version="0.1",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=load_required_dependencies(),
)
