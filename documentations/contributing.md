# Contributing

First, you might want to see the basic ways to [Help](help.md).

## Developing

If you already cloned the repository and you know that you need to deep dive in the code, here are some guidelines to set up your environment.

### Poetry

**SQLSimple** uses <a href="https://python-poetry.org/" class="external-link" target="_blank">Poetry</a> to build, package, and publish the project.

You can learn how to install it in the <a href="https://python-poetry.org/docs/#installation" class="external-link" target="_blank">Poetry</a> documentation.

After having Poetry available, you can install the development dependencies:

```shell
$ poetry install
```

It will also create a virtual environment automatically and will install all the dependencies and your local SQLSimple in it.

### Poetry Shell

To use your current environment, and to have access to all the tools in it (for example `pytest` for the tests) enter into a Poetry Shell:

```shell
$ poetry shell
```

That will set up the environment variables needed and will start a new shell with them.

### Python

SQLSimple supports Python 3.6 and above, but for development you should have at least Python 3.8.

## Documentation

The documentation uses <a href="https://www.mkdocs.org/" class="external-link" target="_blank">MkDocs</a> with <a href="https://squidfunk.github.io/mkdocs-material/" class="external-link" target="_blank">Material for MkDocs</a>.

All the documentation is in Markdown format in the directory `./documentations`.
