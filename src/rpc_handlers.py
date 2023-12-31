from src import utils

STATUS_OK = 200
STATUS_NOT_FOUND = 404
STATUS_CONFLICT = 409

# Map RPC types with handlers
REQUEST_MAP = {
    "poll": lambda n, body: poll(),
}


def poll():
    """
    Reads poll request from seed server, responds with OK
    :return: string of response
    """
    resp_header = {"status": STATUS_OK}

    return utils.create_request(resp_header, {})
