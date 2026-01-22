# Contributing

How to work on QuickMQ.

## GitLab Setup

All contributions should be done through branches, pushing to main is not allowed. If you have a large proposal or many ideas, it may be best to first [open an issue](https://gitlab.ssec.wisc.edu/SDS/rabbitmq/ssec_amqp/-/issues) to discuss about the implementation.

### Cloning the repo

If you have permission, you can get started by cloning the repo with

```bash
git clone git@gitlab.ssec.wisc.edu:SDS/rabbitmq/ssec_amqp.git
cd ssec_amqp
```

### Forking the repo

If you do not have permission to directly clone the repo, the best option is to following the [GitLab forking workflow](https://docs.gitlab.com/ee/user/project/repository/forking_workflow.html).

## Local Environment Setup

QuickMQ's development relys heavily on [hatch](https://hatch.pypa.io/1.12/) to provide environment isolation for development and testing.

Hatch can be installed with `pip install hatch` or a variety of [other methods](https://hatch.pypa.io/1.12/install/).

After installing, the `hatch` command can be used for a variety of things from running tests to linting the package.

### Virtual Environments

By default hatch will install the virtual environment under your home directory. If you prefer to have the env "closer" to the project, use `hatch config set dirs.env.virtual .hatch`. This will put all the virtual environments hatch creates in the .hatch folder at the projects root directory.

### Development Environment

To install QuickMQ in development mode, along with all its dependencies, use `hatch shell dev`.

### Code Styling and Quality Checks

QuickMQ follows consistent code styling using [`hatch fmt`](https://hatch.pypa.io/1.9/cli/reference/#hatch-fmt). Before committing your changes, it can be helpful to run the command to make sure everything is in order.

QuickMQ also heavily uses type annotations to provide library users completion, etc. To validate your type annotations, QuickMQ uses [mypy](https://www.mypy-lang.org) which can easily be run with `hatch run types:check`.

You don't have to worry too much about if you forget, because [CI](#CI) won't.

## Running Tests

Unit tests are run using `hatch test`. Tests can be run on a variety of Python interpreters automagically using hatch, see `hatch test --help` for all options.

Integration tests are run using `hatch run integration:test`. Docker or Podman will need to be installed to run the integration tests.

### Failing Tests

#### Unit Tests

Breaking unit tests are not the end of the world. It is relatively normal for an internal change to break a couple of unit tests. If the change really is internal (doesn't require a major version release) then simply update the unit tests to use the new change.

#### Integration Tests

On the other hand, breaking integration tests means that the public API changed somehow. If this was done on purpose, [release a new major version](#releases) and update the tests. If it wasn't done on purpose, fix your code to get the integration tests passing.

## CI

Continuous Integration to check for code quality and run tests is done whenever you push your changes to a branch, or create a merge request. If CI fails, you can find out why through the [GitLab GUI](https://docs.gitlab.com/ee/ci/quick_start/#view-the-status-of-your-pipeline-and-jobs), fix the problems locally, and finally, commit and push your changes again.

If you want to check to see if CI will pass for your commit use the following commands:

```bash
hatch fmt
hatch run types:check
hatch test -ac
hatch run integration:test
```

The one exception to this is a test for compatability with python 3.6 (the [minimum supported Python version](/README.md#installation-requirements)).

## Releases

This information is not needed for regular contributors.

Once there are [enough changes](https://semver.org/#semantic-versioning-specification-semver) since the last version, it's time to release a new one.

Before creating a release make sure the [docs](/docs) are up to date, and add all changes to the [CHANGELOG](/CHANGELOG)! Unfortunately, this will have to be done manually :(

Once all documentation is merged in `main`, the release process goes as follows:

Update your local branch:

```bash
git checkout main
git pull
```

Create a release branch:

```bash
git checkout -b release
```

Bump the version:

```bash
hatch version {major|minor|patch}
```

Push and merge the release branch:

```bash
git commit -am "bump version"
git push -u origin release -o merge_request.create -o merge_request.target=main -o merge_request.merge_when_pipeline_succeeds -o merge_request.remove_source_branch
```

When (**and only when!**) CI passes for the merge request and the main branch, create and push the new tag:

```bash
git checkout main
git pull
git branch -d release

git tag -am 'x.x.x' x.x.x // Where x.x.x matches the new version from `hatch version`
git push --tags
```

Once the tag is pushed, CI will automatically package and release the new version to PyPi and GitLab.

If CI fails for the tag pipeline, delete the tag from the repo using the GUI and your local repo. Then fix the changes and recreate and push the tag. The git history will be a bit out of order, but it's not the end of the world.
