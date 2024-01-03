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
}

EXPECTED_REQUEST = {
    "poll": (),
    "get_coordinates": (),
}


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


def just_joined(n, body):
    """
    Adds a new node to this node's state if necessry
    :param n: node
    :param body: body of request
    :return: string of response
    """
    resp_header = {"status": STATUS_UPDATE_REQUIRED}
    resp_body = {}
    # check if state of node has been updated since join request
    if n.update_timestamp > body["timestamp"]:
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
            resp_body["leaf_set_larger"] = [
                {"ip": l.addr[0], "port": l.addr[1], "node_id": l.addr}
                for l in n.leaf_set_larger
            ]

        # return this node's (possibly new) routing table rows
        resp_body["routing_table"] = []
        for i in range(body["start_row"], body["end_row"] + 1):
            resp_body["routing_table"].append(
                [
                    {"ip": l.addr[0], "port": l.addr[1], "node_id": l.addr}
                    for l in n.routing_table[i]
                ]
            )
    else:
        resp_header = {"status": STATUS_OK}

    new_link = Link((body["ip"], body["port"]), body["node_id"])

    L2 = utils.params["node"]["L2"] // 2

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
            len(n.leaf_set_larger) < L2
            or n.leaf_set_larger[-1].node_id > new_link.node_id
        ):
            utils.insert_sorted(
                n.leaf_set_larger,
                new_link,
                L2,
                remove=-1,
                comp=lambda x, y: x.node_id > y.node_id,
            )

        # calculate distance to node
        distance = n.get_distance_to(new_link.addr)
        n.id_to_distance[new_link.node_id] = distance

        # add to neighborhood set if appropriate
        if distance < n.get_distance_cached(
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
        new_node_id_digits = list(utils.get_digits(new_link.node_id))
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

    n.event_queue.put(update)

    return utils.create_request(resp_header, resp_body)
