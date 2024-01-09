import os
import sys

sys.path.append(".")
os.chdir("..")
from src import utils

if __name__ == "__main__":
    for i in range(
        utils.params["testing"]["initial_port"],
        utils.params["testing"]["initial_port"]
        + utils.params["testing"]["total_nodes"],
    ):
        try:
            response = utils.ask_peer(("", i), "debug_state", {})
            print(response)
        except:
            pass
