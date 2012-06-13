import json

import heap
import slave
import master

class Node(object):
    '''
    An instance of this class represents a machine in our distributed system
    cluster.
    '''

    Modes = ("slave",
            "full-master",
            "assign-only-master",
            "combine-only-master",
            )

    def __init__(self, mode, config_file):
        if mode not in self.Modes:
            raise Exception("Unacceptable mode for a node: " + mode)

        self.mode = mode
        try:
            with open(config_file) as config_file_handler:
                self.config = json.load(config_file_handler)
        except IOError as e:
            raise Exception("Failed to load config file " + config_file)

        if self.mode == "slave":
            try:
                master_combine_ip = self.config["master-combine"]["ip"]
                master_combine_name = self.config["master-combine"]["name"]
                master_assign_ip = self.config["master-assign"]["ip"]
                master_assign_name = self.config["master-assign"]["name"]
            except KeyError as ke:
                raise Exception("Master configurations missing.")
                return

            master_assign = master.Master(ip = master_assign_ip,
                                          name = master_assign_name)
            master_combine = master.Master(ip = master_combine_ip,
                                           name = master_combine_name)
            this_node = slave.Slave(master_combine = master_combine,
                                    master_assign = master_assign)
            this_node.start_work()

        elif self.mode == "full-master":
            slave_configs = self.config["slaves"]
            slaves = []
            for slave_config in slave_configs:
                try:
                    slaves.append(slave.Slave(slave_config["ip"],
                                            slave_config["name"]))
                except KeyError:
                    raise Exception("Slave configurations incomplete.")
            else:
                raise Exception("Slave configurations missing.")
            this_node = master.Master(slaves = slaves, mode = "full")
            this_node.start_work()

        elif self.mode == "assign-only-master":
            slave_configs = self.config["slaves"]
            slaves = []
            for slave_config in slave_configs:
                try:
                    slaves.append(slave.Slave(slave_config["ip"],
                                            slave_config["name"]))
                except KeyError:
                    raise Exception("Slave configurations incomplete.")
            else:
                raise Exception("Slave configurations missing.")
            this_node = master.Master(slaves = slaves, mode = "assign-only")
            this_node.start_work()

        elif self.mode == "combine-only-master":
            slave_configs = self.config["slaves"]
            slaves = []
            for slave_config in slave_configs:
                try:
                    slaves.append(slave.Slave(slave_config["ip"],
                                            slave_config["name"]))
                except KeyError:
                    raise Exception("Slave configurations incomplete.")
            else:
                raise Exception("Slave configurations missing.")
            this_node = master.Master(slaves = slaves, mode = "combine-only")
            this_node.start_work()
