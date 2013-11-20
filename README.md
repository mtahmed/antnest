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

# Contact

- Muhammad Tauqir Ahmad
- muhammad.tauqir.ahmad@gmail.com
- [csclub.uwaterloo.ca/~mtahmed](http://csclub.uwaterloo.ca/~mtahmed)

## TODO

- Add license info.
- Testing


[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/mtahmed/antnest/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

