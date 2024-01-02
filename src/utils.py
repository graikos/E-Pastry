import threading
import socket
import math
import logging
import json
from os import environ

# get configuration settings from params.json
with open("config/params.json") as f:
    params = json.load(f)

log = logging.getLogger(__name__)

log.info("Loaded params")


def get_id(key, hash_func):
    """
    Returns ID of key
    :param key: string to hash and get ID
    :param hash_func: hash function to use
    :return: int ID corresponding to given key
    """
    key = key.encode("utf-8")

    ring_size = params["ring"]["bits"]
    # truncate to necessary number of bytes and get ID
    trunc_size = math.ceil(ring_size / 8)
    res_id = int.from_bytes(hash_func(key).digest()[-trunc_size:], "big")

    return res_id % 2**ring_size


def get_id_digits(id_int):
    """
    Accepts an id as an integer and creates a generator of the digits of the id in the base being used, from most to least important
    :param id_int: the id as an integer
    :return: the generator
    """
    max_len = math.ceil(params["ring"]["bits"] / params["ring"]["b"])
    for i in range(1, max_len + 1):
        q = id_int // 2 ** (params["ring"]["bits"] - i * params["ring"]["b"])
        yield q % 2 ** params["ring"]["b"]


def get_longest_common_prefix(digits1, digits2):
    """
    Returns the longest common prefix for two ids
    Both iterables must be of the same length
    :param digits1: an iterable containing the first set of digits
    :param digits2: a generator containing the second set of digits
    :return: the longest common prefix
    """
    count = 0
    for digit in digits1:
        if digit != next(digits2):
            break
        count = count + 1

    return count


def get_ip():
    """
    Returns the local IP address of the machine
    :return: the IP address
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except (socket.error, socket.timeout):
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def create_request(header_dict, body_dict):
    """
    Creates request from passed header and body
    :param header_dict: dictionary of header
    :param body_dict: dictionary of body
    :return:
    """
    request_dict = {"header": header_dict, "body": body_dict}
    request_msg = json.dumps(request_dict, indent=2)

    return request_msg


def haversine(coord1, coord2):
    """
    Calculates the Haversine distance between two sets of coordinates
    :param coord1: tuple containing first set of (latitude, longitude)
    :param coord2: tuple containing second set of (latitude, longitude)
    """
    # Radius of Earth in kilometers
    R = 6371.0

    lat1, lon1 = coord1
    lat2, lon2 = coord2

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance


class RWLock:
    """
    Implements a readers-writer block with no priorities
    """

    def __init__(self):
        """
        Initializes two mutexes (reader and writer mutex) and the reader counter
        """
        self.r_lock = threading.Lock()
        self.w_lock = threading.Lock()
        self.readers = 0

    def r_enter(self):
        """
        Used for reader to enter critical region
        :return: None
        """
        self.r_lock.acquire()
        self.readers += 1
        if self.readers == 1:
            self.w_lock.acquire()
        self.r_lock.release()

    def r_leave(self):
        """
        Used for reader to leave critical region
        :return: None
        """
        self.r_lock.acquire()
        self.readers -= 1
        if not self.readers:
            self.w_lock.release()
        self.r_lock.release()

    def r_locked(self):
        return self.r_lock.locked()

    def w_enter(self):
        """
        Used for writer to enter critical region
        :return: None
        """
        self.w_lock.acquire()

    def w_leave(self):
        """
        Used for writer to leave critical region
        :return: None
        """
        self.w_lock.release()

    def w_locked(self):
        return self.w_lock.locked()
