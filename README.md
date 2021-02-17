# delphixpy-examples
These are some example python scripts for those getting started
with the Delphix delphixpy python module.

## Changes in this Branch
- This branch requires Python3. All enhancements and break fixes will
  be committed to this branch and will eventually become the
  ``master`` branch.
- We have a new format for dxtools.conf. If you used this repo in the
  past, please be aware of the new format.
- All connections use HTTPS by default. Please refer to
  lib/get\_session.py if using this repo in a production environment.
- Migrated from Delphix API version 1.8.0 to 1.10.2.

## Wait... What's Delphix?
In the most simplest answer, [Delphix](http://www.delphix.com) is an
awesome software that allows you to provision full multi-terabyte
databases and applications in minutes. It is far more than that, but
that's why Google and this [blog](adam.today) exist.

## What is delphixpy?
delphixpy is a python module created by Delphix to enable users to
directly invoke the Delphix API via python.

## Where can I get delphixpy?
delphixpy is available on PyPy, so you can install it by invoking pip

    pip3 install delphixpy

## How do I use these examples?
Clone this repository to your system where python is installed. Then
install the pip packages in the requirements.txt file:

    pip3 install --upgrade -r requirements.txt

Once that is complete, you are ready to use the scripts with your
Delphix environment. Each of the scripts have POSIX compliant
help. The options are also explained along with examples. I am going
to explain more on these scripts in @CloudSurgeon and on [the Delphix
community page](https://community.delphix.com)

## <a id="contribute"></a>Contribute

1.  Fork the project.
2.  Make your bug fix or new feature.
3.  Add tests for your code.
4.  Send a pull request.

Contributions must be signed as `User Name <user@email.com>`. Make
sure to [set up Git with user name and email
address](https://git-scm.com/book/en/v2/Getting-Started-First-Time-Git-Setup). Bug
fixes should branch from the current stable branch

### Formatting

This repository uses the `tox` and `pre-commit` tools to run
autoformatters on the entire repository. These two tools are the
industry standard for Python. The goal of these formatters is to
delegate issues of formatting to the machine so that develeopers and
code-reviewers can focus on more important things.

The two main autoformatters that we use are
 - `black`: General Python formatting
 - `isort`: Import sorting

## Running the formatting

The formatting is automatically run remotely on every Github pull
request and on every push to Github.

It is possible to run these locally in two ways. Automatically before
every push and manually.

To have the checks run automatically before every push you can enable
`pre-commit`.

```
tox
.tox/format/bin/pre-commit install --hook-type pre-push
```

To run the checks manually:
On the entire repository
```
	tox -- --all-files
```
on a specific file
```
	tox -- --file <file-name>
```
On every file in the most recent commit
```
    git diff-tree --no-commit-id --name-only -r HEAD | xargs tox -- --files
```

## Something neat worth noting
Each of the scripts leverage
[docopt](https://github.com/docopt/docopt), which is a great module
for parsing POSIX compliant help documentation as arguments. It's
really awesome.
