# Contributing to pantab

## Cloning the Repository

For simplicity you should first fork the repository on GitHub, then locally run

```sh
git clone https://github.com/your-user-name/pantab.git
cd pantab
git remote add upstream https://github.com/innobi/pantab.git
```

to set pantab as the upstream project locally.

## Setting up an Environment

A conda environment file containing development dependencies is available in the root of the package, so simply run

```sh
conda env create -f environment.yml
```

The first time you work with the source code. Then activate your virtual environment any time you are working with the code

```sh
conda activate pantab
```

Additionally you should install the Tableau Hyper API. As of writing this is not available on PyPi, so you should install the appropriate package for your platform listed on the [Hyper API Download Page](https://www.tableau.com/support/releases/hyper-api/0.0.9273#esdalt)

## Building and Modifying Documentation

Documentation is housed in the `doc` folder of the project. When in that directory simply run `make html` to generate the documentation.

## Making Code Changes

### Reference an Issue

While minor documentation edits / typo fixes can be pushed without an issue, for all other changes you should first open an issue on GitHub. This ensures that the topic can be discussed in advance and is a requirement for towncrier to generate our whatsnew notes (more on this later).

### Creating your local branch

In your local `pantab` copy make sure that you have the latest and greatest updates before creating a dedicated branch for development.

```sh
git checkout master
git pull upstream master
git checkout -b a-new-branch
```

### Building the Project

`pantab` uses C extension(s) to optimize performance, so you will need to build your local environment before attempting to import and use the package. To do this run

```sh
python setup.py build_ext --inplace
```

From the project root. Note that this will fail without a C compiler - if you don't have one installed check out the appropriate documentation from the [Python Developer Guide](https://devguide.python.org/setup/#compile-and-build) for your platform.

### Creating tests and running the test suite

Tests are required for new changes and no code will be accepted without them. You should first set up your test in the appropriate module in the `pantab/tests` directory. You can then run the test suite with

```sh
pytest pantab
```

### Style guidelines for code changes

Note that `pantab` uses `black`, `flake8` and `isort` to manage code style. You can run these as follows:

```sh
black pantab
flake8 pantab
isort **/*.py
```

### Annotations

New code development should come bundled with type annotations. Be sure to check any new annotations with

```sh
mypy pantab
```

### Commit your changes

Assuming all of the checks above pass on your code, go ahead and commit those changes. 

```sh
git add <...>
git commit -m "<Message for your change>"
```

### Running / Adding benchmarks

For performance critical code or improvements, you may be asked to add benchmark(s). These can be found in the `benchmarks` folder. To run the suite, execute

```sh
asv continuous upstream/master HEAD
```

to compare results to the latest commit on your branch. Output should be copy/pasted into any pull request.

### Pushing to GitHub

You should push your local changes to your fork of pantab

```sh
git push origin your-branch-name
```

And create a pull request to pantab from there.
