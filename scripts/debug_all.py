import os
import sys

sys.path.append(".")
os.chdir("..")
from src import utils

total = 0
if __name__ == "__main__":
    for i in range(
        utils.params["testing"]["initial_port"],
        utils.params["testing"]["initial_port"]
        + utils.params["testing"]["total_nodes"],
    ):
        try:
            response = utils.ask_peer(("", i), "debug_state", {})
            if response:
                total += 1
        except:
            pass

    print("Total nodes: ", total)
