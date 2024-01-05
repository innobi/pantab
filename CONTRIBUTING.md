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
# add our lints as part of your local git workflow
pre-commit install
```

The first time you work with the source code. Then activate your virtual environment any time you are working with the code

```sh
conda activate pantab-dev
```

## Building and Modifying Documentation

Documentation is housed in the `doc` folder of the project. When in that directory simply run `make html` to generate the documentation.

## Making Code Changes

### Reference an Issue

While minor documentation edits / typo fixes can be pushed without an issue, for all other changes you should first open an issue on GitHub. This ensures that the topic can be discussed in advance and is a requirement for towncrier to generate our whatsnew notes (more on this later).

### Creating your local branch

In your local `pantab` copy make sure that you have the latest and greatest updates before creating a dedicated branch for development.

```sh
git checkout main
git pull upstream main
git checkout -b a-new-branch
```

### Building the Project

For an editable install of pantab you can simply run `pip install -ve .` from the project root.

### Creating tests and running the test suite

Tests are required for new changes and no code will be accepted without them. You should first set up your test in the appropriate module in the `pantab/tests` directory. You can then run the test suite with

```sh
pytest pantab
```

### Style guidelines for code changes

Note that `pantab` uses `black`, `flake8` and `isort` to manage code style. Simply run pre-commit. If pre-commit modifies files, simply add them and run pre-commit again. Note, if you've already run `pre-commit install` it will automatically run before every commit regardless.

```sh
pre-commit
```

### Annotations

New code development should come bundled with type annotations. Be sure to check any new annotations with

```sh
mypy pantab
```

### Adding a whatsnew entry

Every change should come with a [news fragment](https://github.com/hawkowl/towncrier#news-fragments) placed in the `pantab/newsfragments/` folder. Please use the issue number as the file name and the appropriate news fragment extension. So for instance, if you are closing issue #100 with a new feature, the file should be named `100.feature` and contain a short description of the change being made.

Ultimately before release all news fragments will be compiled with `towncrier` to create the whatsnew entry.

### Commit your changes

Assuming all of the checks above pass on your code, go ahead and commit those changes.

```sh
git add <...>
git commit -m "<Message for your change>"
```

### Running / Adding benchmarks

For performance critical code or improvements, you may be asked to add benchmark(s). These can be found in the `benchmarks` folder. To run the suite, execute

```sh
asv continuous upstream/main HEAD
```

to compare results to the latest commit on your branch. Output should be copy/pasted into any pull request.

### Pushing to GitHub

You should push your local changes to your fork of pantab

```sh
git push origin your-branch-name
```

And create a pull request to pantab from there.
