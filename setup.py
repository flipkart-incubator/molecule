from setuptools import find_packages, setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="molecule",
    version="0.1.0",
    description="An scalable ML Platform to help you build, train, and deploy models",
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/flipkart-incubator/molecule",
    author="Suvigya Vijay",
    author_email="suvigyavijay@gmail.com",
    license="Apache License 2.0",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "pyyaml",
        "google-cloud-logging",
        "google-cloud-storage",
        "google-cloud-bigquery",
        "zmq",
        "flask",
        "flask_cors",
        "python-crontab",
        "pandas",
        "pyarrow",
        "gcsfs",
        "pymongo",
        "Flask-PyMongo",
    ],
    extras_require={
        "dev": ["pytest>=7.0", "twine>=4.0.2"],
    },
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "molecule = molecule.cli:main",
            "molecule-server = molecule.start_platform:main",
            "molecule-spawner = molecule.spawner:main",
        ],
    },
    package_data={'': ['*.r', '*.R']},
    include_package_data=True,
)
