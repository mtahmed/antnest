'''
Schedulers

Each scheduler takes the number of machines, the speed for each machine,
the set of jobs
(in scheduling terminology, jobs) that are to be assigned to (scheduled on) that
machine.

If the list of speeds is empty, it is assumed that the machines are identical.

NOTE: This file uses the scheduling terminology, not consistent with the rest of
      the system.
'''
# Custom imports
from utils.heap import Heap


class MinMakespan():
    '''A scheduler that aims to minimize the makespan of the jobs.

    The algorithm is based on the scheduling algorithm for minimizing makespan
    on parallel related machines, usually represented using the familiar
    scheduling problem notation Q||C_max. It is a 2-approximation list
    scheduling algorithm for the problem.
    '''
    def __init__(self, machines=0, speeds=[], jobs=[]):
        '''
        :param machines: The number of machines.
        :param speeds: The speeds for each of the machines.
        :param jobs: The (initial) set of jobs to be scheduled.

        NOTE: Each job in jobs must have a job_size attribute.
        '''
        if len(speeds) == 0:
            self.speeds = [1 for _ in range(machines)]
        elif len(speeds) != machines:
            raise ValueError("speeds should be the same length as machines or"
                             "empty")
        else:
            self.speeds = speeds
        self.machines = machines

        # assignments[machine] = list of jobs assigned to machine
        self.assignments = [[] for _ in range(machines)]
        # A min-heap of the loads on the machines.
        self.loads_heap = Heap([(i,0) for i in range(machines)],
                               key=lambda x: x[1])

        # Now schedule the jobs.
        for job in jobs:
            machine = self.schedule_job(job)

    def schedule_job(self, job):
        '''Schedule the job according to the current loads.

        :param job: The job to be scheduled.
        :returns: The machine the job get's scheduled on.
        :rtype: int representing the machine
        '''
        machine, load = self.loads_heap.pop()
        self.assignments[machine].append(job)
        self.loads_heap.push((machine, load + job.job_size))

        return machine

    def add_machine(self, speed=1):
        self.speeds.append(speed)
        self.loads_heap.push((self.machines, 0))
        self.machines += 1
