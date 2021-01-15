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
delphixpy is available on PyPy, so you can install it by invoking pip

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


### Unittests

The unittests are run through the `tox` tool. To run in Python2 run:
```
tox -e py27
```

To run in Python3 run:
```
tox -e py36
```

## Statement of Support

This software is provided as-is, without warranty of any kind or
commercial support through Delphix. See the associated license for
additional details. Questions, issues, feature requests, and
contributions should be directed to the community as outlined in the
[Delphix Community
Guidelines](https://delphix.github.io/community-guidelines.html).

## License

This is code is licensed under the Apache License 2.0. Full license is available [here](./LICENSE).
