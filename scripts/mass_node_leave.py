import os
from random import choice
import sys

sys.path.append(".")
os.chdir("..")
from src import utils

node_details = range(
    utils.params["testing"]["initial_port"],
    utils.params["testing"]["initial_port"] + utils.params["testing"]["total_nodes"],
)
total_nodes = list(node_details)
total_removed = 0

while (
    total_removed * 100 / len(node_details)
    < utils.params["testing"]["percentage_to_remove"]
):
    n = choice(total_nodes)
    if utils.ask_peer(("", n), "leave_ring", {}):
        total_nodes.remove(n)
        total_removed += 1
        print(f"\rTotal removed: {total_removed}" + " " * 20, end="")

print()
