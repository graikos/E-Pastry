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

all_keys = set()

while (
    total_removed * 100 / len(node_details)
    < utils.params["testing"]["percentage_to_remove"]
):
    n = choice(total_nodes)
    response =  utils.ask_peer(("", n), "debug_leave_ring", {})

    if not response:
        continue

    if response["header"]["status"] == 200:
        all_keys.update(response["body"]["keys"])
        total_nodes.remove(n)
        total_removed += 1
        print(f"\rTotal removed: {total_removed}" + " " * 20, end="")

print()

with open("scripts/lost_keys.dat", "w") as f:
    f.writelines([f"{k}\n" for k in all_keys])
