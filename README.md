# antnest

A distrubuted system implementation aimed to be quick to deploy and simple to
use. All it takes to start using it is to write your problem as 3 functions
on the _master_ side: `split()`, `combine()` and `processor()`.

This works without the _worker_ (slave) nodes having any knowledge that is
usually required for an RPC call.

## Installation

On all the masters and slaves, clone the antnest repository:

```bash
git clone https://github.com/mtahmed/antnest.git
cd antnest
```

## Config

On the slaves, add a JSON config file, telling the slaves about how to find
masters:

```bash
cat > config/$(hostname)-slave-config.json
{
  "masters": [
    {
      "ip": "192.168.0.1",
      "hostname": "master"
    }
  ]
}
^D
```

_NOTE_: the `$(hostname)-slave-config.json` used as the filename for the config
file. That's the convention used and that's what the node will look for when
it's started up.


## Usage

Start the slave with the help `commands/start-slave.py` script:

```bash
python commands/start-slave.py
```

Do the same for the master:

```bash
python commands/start-master.py
```

Then write a job (see below on how to do that) and tell the master to run the
job:

```bash
python commands/create_job.py -j jobs/reverse_strings.py
```

The jobs are, by convention, defined in `jobs/` directory as python files.
`reverse_strings.py` is one of the sample jobs provided.


## Jobs

Jobs are what define a complete unit of meaningful work. It defines the inputs,
the processing, splitting, and combining of subunits of work (taskunits).

Jobs are defined in `.py` files and can be started on a master using the
`create_job.py` script (see above).

A job must define at least a `processor` function which will take the input and
process it to produce the desired result. A job can also define a `split`
function which tells the master how to split a job into taskunits and a
`combine` method which tells the master how to combine processed taskunits to
produce the final result.

Example:

```python
def processor(self, string):
    return string[::-1]

def split(self, input_data, processor):
    import taskunit
    input_lines = input_data.split('\n')
    for input_line in input_lines:
        yield taskunit.TaskUnit(data=input_line, processor=processor)

def combine(self):
    for t in self.taskunits:
        print(t.result)
    return

input_data = 'hello\nworld'
```

Here the `input_data` will be split at newline and so will produce exactly 2
taskunits (one with `input_data` being `'hello'` and one with `input_data` being
`'world'`). The processor will reverse each string on the slave nodes to produce
the results `'olleh'` and `'dlrow'`. The combiner simply prints the results
(boring, I know).


## Testing

Requires `pytest` for testing. Simply run `py.test` from the root directory of
the repository to run the tests. TODO: Test coverage

# Contact

- Muhammad Tauqir Ahmad
- muhammad.tauqir.ahmad@gmail.com
- [csclub.uwaterloo.ca/~mtahmed](http://csclub.uwaterloo.ca/~mtahmed)

# LICENSE

[WTFPL](http://www.wtfpl.net/) License

But please do let me know and acknowledge me if you do use it so I can feel good
about myself.
