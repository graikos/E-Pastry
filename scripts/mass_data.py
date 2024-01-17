import json
from sys import argv
import os
from random import choice
import sys
from time import time, sleep

sys.path.append(".")

s = time()

with open(argv[1]) as f:
    data = json.load(f)

os.chdir("..")
from src import utils

if argv[2] in ("insert", "i"):
    req = "find_and_store_key"
    req_body = lambda e: {"key": e, "value": data[e]}
    insert = True
    print("Inserting keys...")
elif argv[2] in ("lookup", "l"):
    req = "find_key"
    req_body = lambda e: {"key": e}
    insert = False
    print("Looking up keys...")
else:
    print("Expected request type (i, l) as second argument.")
    exit(1)

skip_keys = None

try:
    skip_keys = argv[3].lower() == "skip"
except IndexError:
    pass

stop_at = None

try:
    stop_at = int(argv[4])
except IndexError:
    pass

delay = None

try:
    delay = int(argv[5])
except IndexError:
    pass


try:
    if insert or not skip_keys:
        raise FileNotFoundError
    with open("scripts/lost_keys.dat", "r") as f:
        lost_keys = set([k.strip() for k in f.readlines()])
except FileNotFoundError:
    lost_keys = set()

if delay is not None and s + delay > time():
    sleep(delay - (time() - s))

failed_req = 0
total_req = 0

ports = range(
    utils.params["testing"]["initial_port"],
    utils.params["testing"]["initial_port"] + utils.params["testing"]["total_nodes"],
)

data = {k: v for k, v in data.items() if k not in lost_keys}

for event in data:
    response = None
    while response is None:
        response = utils.ask_peer(
            ("", choice(ports)), req, req_body(event), custom_timeout=0.1
        )

    if response["header"]["status"] not in (200, 300):
        failed_req += 1
    total_req += 1

    print(
        "\rTried {}/{} keys; Fail percentage: {:4f}%".format(
            total_req,
            len(data) if stop_at is None or stop_at > len(data) else stop_at,
            failed_req * 100 / total_req,
        )
        + 20 * " ",
        end="",
    )

    if total_req == stop_at:
        break

print()
