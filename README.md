# delphixpy-examples
These are some example python scripts I put together to serve as
examples for those getting started with Delphix the delphixpy python
module.

## Thanks
First, a lot of thanks to Corey Brune
([@mcbrune](https://github.com/mcbrune)) for all of his contributions
that make this spaghetti look decent.

## Wait... What's Delphix?
In the most simplest answer, [Delphix](http://www.delphix.com) is an
awesome software that allows you to provision full multi-terabyte
databases and applications in minutes. It is far more than that, but
that's why Google and this [blog](adam.today) exist.

## What is delphixpy?
delphixpy is a python module created by Delphix to enable users to
directly invoke the Delphix API via python.

## Where can I get delphixpy?
delphixpy is available on PyPI, so you can install it by invoking pip

    pip install delphixpy

## How do I use these examples?
Clone this repository to your system where python is installed. Then
install the pip packages in the requirements.txt file:

    pip install --upgrade -r requirements.txt

Once that is complete, you are ready to use the scripts with your
Delphix environment. Each of the scripts have POSIX compliant
help. The options are also explained along with examples. I am going
to explain more on these scripts in my blog and on [the Delphix
community page](https://community.delphix.com)

## Something neat worth noting
Each of the scripts leverage
[docopt](https://github.com/docopt/docopt), which is a great module
for parsing POSIX compliant help documentation as arguments. It's
really awesome.

## Contribute

All contributors are required to sign the Delphix Contributor
Agreement prior to contributing code to an open source
repository. This process is handled automatically by
[cla-assistant](https://cla-assistant.io/). Simply open a pull request
and a bot will automatically check to see if you have signed the
latest agreement. If not, you will be prompted to do so as part of the
pull request process.

This project operates under the [Delphix Code of
Conduct](https://delphix.github.io/code-of-conduct.html). By
participating in this project you agree to abide by its terms.

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

## License

This is code is licensed under the Apache License 2.0. Full license is available [here](./LICENSE).
