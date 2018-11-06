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
