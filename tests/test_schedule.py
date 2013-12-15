import schedule


scheduler = schedule.MinMakespan()


class FakeJob:
    def __init__(self, job_size=1):
        self.job_size = job_size
        return


def test_num_machines():
    assert scheduler.machines == 0


def test_add_machine():
    scheduler.add_machine()
    assert scheduler.machines == 1


def test_add_machines():
    scheduler.add_machine(2)
    scheduler.add_machine(3)
    scheduler.add_machine(4)
    scheduler.add_machine(5)
    assert scheduler.machines == 5


def test_schedule_job():
    fakejob = FakeJob()
    machine = scheduler.schedule_job(fakejob)
    assert machine == 0


def test_schedule_jobs():
    fakejob1 = FakeJob()
    fakejob2 = FakeJob()
    fakejob3 = FakeJob()
    fakejob4 = FakeJob()
    machine1 = scheduler.schedule_job(fakejob1)
    machine2 = scheduler.schedule_job(fakejob2)
    machine3 = scheduler.schedule_job(fakejob3)
    machine4 = scheduler.schedule_job(fakejob4)
    machines = [machine1, machine2, machine3, machine4]
    machines.sort()
    assert machines == [1, 2, 3, 4]
