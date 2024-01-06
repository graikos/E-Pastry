from src import utils
from src.Link import Link

STATUS_OK = 200
STATUS_NOT_FOUND = 404
STATUS_CONFLICT = 409
STATUS_UPDATE_REQUIRED = 432

# Map RPC types with handlers
REQUEST_MAP = {
    "poll": lambda n, body: poll(),
    "get_coordinates": lambda n, body: get_coordinates(n),
    "just_joined": lambda n, body: just_joined(n, body),
    "join": lambda n, body: join(n, body),
    "locate_closest": lambda n, body: locate_closest(n, body),
    "find_key": lambda n, body: find_key(n, body),
    "lookup": lambda n, body: lookup(n, body),
    "debug_state": lambda n, body: debug_state(n),
}

"""
Expected parameters for request body per request type
Format:
*) key (request type): (list of expected parameters)
*) key (request type): (additional bool request header, (list of parameters if true), (list of parameters if false)
"""
EXPECTED_REQUEST = {
    "poll": (),
    "get_coordinates": (),
    "just_joined": (
        (
            "in_route",
            (
                "timestamp",
                "ip",
                "port",
                "node_id",
                "is_first",
                "is_last",
                "start_row",
                "end_row",
            ),
            (
                "ip",
                "port",
                "node_id",
            ),
        )
    ),
    "join": ("node_id", "initial", "start_row"),
    "locate_closest": ("key",),
    "find_key": ("key",),
    "lookup": ("key",),
    "debug_state": (),
}


## RPCs for debugging
def debug_state(n):
    """
    Prints the state of the node
    :param n: node
    :param body: body of request
    :return: string of response
    """
    print("--------------------------------")
    print(f"Node ID: {n.node_id_digits}")
    print(f"Neighborhood set: {n.neighborhood_set}")
    print(f"Leaf set smaller: {n.leaf_set_smaller}")
    print(f"Leaf set greater: {n.leaf_set_greater}")
    print(f"Routing table: {n.routing_table}")
    print("--------------------------------")
    return utils.create_request({"status": STATUS_OK}, {})


## RPCs that only read from the node's state
def poll():
    """
    Reads poll request from seed server, responds with OK
    :return: string of response
    """
    resp_header = {"status": STATUS_OK}

    return utils.create_request(resp_header, {})


def get_coordinates(n):
    """
    Returns the coordinates of the node
    :param n: node
    :return: string of response
    """
    resp_header = {"status": STATUS_OK}
    resp_body = {"latitude": n.latitude, "longitude": n.longitude}

    return utils.create_request(resp_header, resp_body)


def locate_closest(n, body):
    """
    Finds the closest node to the key and returns its value
    Can be called by find_key RPC, or join RPC for special join message
    :param n: node
    :param body: body of request
    :return: string of response
    """
    closest = n.locate_closest(body["key"])

    if closest is not None:
        resp_header = {"status": STATUS_OK}
        resp_body = closest
    else:
        resp_header = {"status": STATUS_NOT_FOUND}
        resp_body = {}

    return utils.create_request(resp_header, resp_body)


def join(n, body):
    """
    Handles a join request from a new node
    :param n: node
    :param body: body of request
    :return: string of response
    """
    resp_body = {}

    # if initial, append neighborhood set to response
    if body["initial"]:
        resp_body["neighborhood_set"] = [
            {"ip": m.addr[0], "port": m.addr[1], "node_id": m.node_id}
            for m in n.neighborhood_set
        ]

    common_prefix = utils.get_longest_common_prefix(
        n.node_id_digits, utils.get_id_digits(body["node_id"])
    )

    # append appropriate rows of routing table to response
    resp_body["routing_table"] = [
        [
            {"ip": m.addr[0], "port": m.addr[1], "node_id": m.node_id}
            if m is not None
            else None
            for m in n.routing_table[i]
        ]
        for i in range(body["start_row"], common_prefix + 1)
    ]

    # append info of this node
    resp_body["node_info"] = {}
    resp_body["node_info"][n.node_id] = {
        "timestamp": n.update_timestamp,
        "is_first": body["initial"],
        "start_row": body["start_row"],
        "end_row": common_prefix,
    }

    # update start row for next node in route
    resp_body["start_row"] = common_prefix + 1

    # forward request to next node
    result = n.handle_join(body["node_id"], resp_body["start_row"])

    if result is None:
        resp_header = {"status": STATUS_NOT_FOUND}
        return utils.create_request(resp_header, {})

    response, resp_body["node_info"][n.node_id]["is_last"] = result

    resp_header = {"status": STATUS_OK}

    # if last, `response` only contains leaf set
    # routing table and node info are in resp_body already
    if not resp_body["node_info"][n.node_id]["is_last"]:
        resp_body["node_info"].update(response["node_info"])
        resp_body["routing_table"].extend(response["routing_table"])

    # forward leaf set through route
    resp_body["leaf_set_smaller"] = response["leaf_set_smaller"]
    resp_body["leaf_set_greater"] = response["leaf_set_greater"]

    return utils.create_request(resp_header, resp_body)


def lookup(n, body):
    """
    Looks up a key in the data of this node
    :param n: node
    :param body: body of request
    :return: string of response
    """
    exists = body["key"] in n.storage
    resp_header = {"status": STATUS_OK if exists else STATUS_NOT_FOUND}
    resp_body = {}
    if exists:
        resp_body["value"] = n.storage[body["key"]]

    return utils.create_request(resp_header, resp_body)


def find_key(n, body):
    """
    Looks through chord for node with key and returns its value
    :param n: the node which should call find_key
    :param body: the request body
    :return: string of response
    """
    value = n.find_key(body["key"])

    resp_header = {}
    resp_body = {}

    if value:
        resp_header["status"] = STATUS_OK
        resp_body["value"] = value
    else:
        resp_header["status"] = STATUS_NOT_FOUND

    return utils.create_request(resp_header, resp_body)


## RPCs that write to the node's state
def just_joined(n, body):
    """
    Adds a new node to this node's state if necessary
    :param n: node
    :param body: body of request
    :return: string of response
    """
    resp_header = {"status": STATUS_UPDATE_REQUIRED}
    resp_body = {}
    # check if state of node has been updated since join request
    if body["in_route"] and n.update_timestamp > body["timestamp"]:
        # if node was first in join, return (possibly new) neighboorhood set
        if body["is_first"]:
            resp_body["neighborhood_set"] = [
                {"ip": l.addr[0], "port": l.addr[1], "node_id": l.addr}
                for l in n.neighborhood_set
            ]
        # if node was last in join, return (possibly new) leaf set
        if body["is_last"]:
            resp_body["leaf_set_smaller"] = [
                {"ip": l.addr[0], "port": l.addr[1], "node_id": l.addr}
                for l in n.leaf_set_smaller
            ]
            resp_body["leaf_set_greater"] = [
                {"ip": l.addr[0], "port": l.addr[1], "node_id": l.addr}
                for l in n.leaf_set_greater
            ]

        # return this node's (possibly new) routing table rows
        resp_body["routing_table"] = []
        for i in range(body["start_row"], body["end_row"] + 1):
            resp_body["routing_table"].append(
                [
                    {"ip": l.addr[0], "port": l.addr[1], "node_id": l.addr}
                    if l is not None
                    else None
                    for l in n.routing_table[i]
                ]
            )
    else:
        resp_header = {"status": STATUS_OK}

    new_link = Link((body["ip"], body["port"]), body["node_id"])

    L2 = utils.params["node"]["L"] // 2

    def update():
        # add to leaf set if appropriate
        if new_link.node_id < n.node_id and (
            len(n.leaf_set_smaller) < L2
            or n.leaf_set_smaller[-1].node_id < new_link.node_id
        ):
            utils.insert_sorted(
                n.leaf_set_smaller,
                new_link,
                L2,
                remove=0,
                comp=lambda x, y: x.node_id > y.node_id,
            )
        elif (
            len(n.leaf_set_greater) < L2
            or n.leaf_set_greater[-1].node_id > new_link.node_id
        ):
            utils.insert_sorted(
                n.leaf_set_greater,
                new_link,
                L2,
                remove=-1,
                comp=lambda x, y: x.node_id > y.node_id,
            )

        # calculate distance to node
        distance = n.get_distance_to(new_link.addr)
        n.id_to_distance[new_link.node_id] = distance

        # add to neighborhood set if appropriate
        if len(n.neighborhood_set) < utils.params["node"][
            "M"
        ] or distance < n.get_distance_cached(
            n.neighborhood_set[-1].node_id, n.neighborhood_set[-1].addr
        ):
            utils.insert_sorted(
                n.neighborhood_set,
                new_link,
                utils.params["node"]["M"],
                remove=-1,
                comp=lambda x, y: n.id_to_distance[x.node_id]
                > n.id_to_distance[y.node_id],
            )

        # calculate common prefix with node id
        new_node_id_digits = list(utils.get_id_digits(new_link.node_id))
        common_prefix = utils.get_longest_common_prefix(
            n.node_id_digits, new_node_id_digits
        )

        # add to routing table if appropriate
        current = n.routing_table[common_prefix][new_node_id_digits[common_prefix]]
        if (
            current is None
            or n.get_distance_cached(current.node_id, current.addr) > distance
        ):
            n.routing_table[common_prefix][new_node_id_digits[common_prefix]] = new_link

        # update timestamp
        n.update_timestamp += 1

    n.event_queue.put(update)

    return utils.create_request(resp_header, resp_body)
