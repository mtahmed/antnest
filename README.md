# PyDistribute

A distrubuted system implementation aimed to be quick to deploy and simple to
use. All it takes to start using it is to write your problem as 3 functions
on the _master_ side: `split()`, `combine()` and `processor()`.

This works without the _worker_ (slave) nodes having any knowledge of what the
function signatures look like for the work that needs to be done or what kind of
input to expect etc.

# Deployment

## Slaves

For each slave, create a config file in the `config/` directory named
`<hostname>-slave-config.json`. Copy the sample `-slave-config`s and edit.
Then in the root directory of the source, run `python commands/start_slave.py`.

## Master

On the master side, just clone the repository and in the root directory of the
source, run `python commands/start_master.py`.

Then one can create a job in the `/jobs/` directory. See `sample_job.py` for
examples on how to do that.

Once the slaves are up and the master is aware of the slaves, send the job to
the master by running `python commands/create_job.py -j jobs/job_file.py`.

That's it.
