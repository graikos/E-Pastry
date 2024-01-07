import multiprocessing
from time import sleep
import os
import sys
sys.path.append(".")
os.chdir("..")
from src import utils


def start_node(porty):
    os.system(f"python3 start_node.py {porty}")


port = utils.params["testing"]["initial_port"]

processes = []
node_details = []

for _ in range(utils.params["testing"]["total_nodes"]):
    new_node = multiprocessing.Process(target=start_node, args=(port,))
    new_node.daemon = True
    processes.append(new_node)
    node_details.append(port)
    new_node.start()
    port += 1
    sleep(.5)

try:
    while True:
        sleep(1)
except KeyboardInterrupt:
    for process in processes:
        process.terminate()
        