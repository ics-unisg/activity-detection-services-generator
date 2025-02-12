# Get Started!

Ready to contribute? Here's how to set up `cepact` for local development.

1. Fork the repo on GitHub.
2. Clone your fork locally.

3. Install the requirements using virtualenv (first create a virtualenv): `pip install -r requirements.txt`

4. To get the necessary developer tools, run `pip install -r requirements-dev.txt`

5. Create a branch for local development. Now you can make your changes locally.

6. When you're done making changes, check that your changes pass several requirements `./code-check.sh`. If needed add/modify tests.
7. Commit your changes and push your branch to GitHub.
8. Submit a pull request through the GitHub website.

# Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.md.
3. The pull request should work for Python 3.12. Make sure that the pipeline passes.


# Deploying

A reminder for the maintainers on how to deploy. Deployment happens manually.
Make sure all your changes are committed (including an entry in HISTORY.md),
and that the pipelines all pass.
Modify the relevant fields in setup.cfg and/or cepact/__init__.py.
Then run::

```
python3 -m pip install --upgrade build
python3 -m build
python3 -m pip install --upgrade twine
python3 -m twine upload dist/*
```