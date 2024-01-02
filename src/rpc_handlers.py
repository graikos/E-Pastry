from src import utils
from src.Link import Link

STATUS_OK = 200
STATUS_NOT_FOUND = 404
STATUS_CONFLICT = 409

# Map RPC types with handlers
REQUEST_MAP = {
    "poll": lambda n, body: poll(),
    "get_coordinates": lambda n, body: get_coordinates(n),
    # "set_routing_table_rows": lambda n, body: set_routing_table_rows(n, body),
}

EXPECTED_REQUEST = {
    "poll": (),
    "get_coordinates": (),
    "set_routing_table_rows": ("rows",),
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
