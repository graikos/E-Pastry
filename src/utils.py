import threading
import socket
import math
import logging
import json
from hashlib import sha1
from os import environ
from types import GeneratorType
from bisect import bisect_left

hash_func = sha1

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
    Iterables can be generators if all the digits don't need to be calculated
    :param digits1: an iterable containing the first set of digits
    :param digits2: an iterable containing the second set of digits
    :return: the longest common prefix
    """
    count = 0
    for digit in digits1:
        if digit != (
            next(digits2) if isinstance(digits2, GeneratorType) else digits2[count]
        ):
            break
        count = count + 1

    return count


def get_numerically_closest(link1, link2, key):
    """
    Returns the id that is numerically closest to the key
    If distance is the same, returns the smaller id
    :param link1: link of first node
    :param link2: link of second node
    :param key: key to compare to
    """
    dist1 = abs(link1.node_id - key)
    dist2 = abs(link2.node_id - key)

    if dist1 < dist2:
        return link1
    elif dist2 < dist1:
        return link2
    else:
        return min((link1, link2), key=lambda x: x.node_id)


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


def create_request(header_dict, body_dict={}):
    """
    Creates request from passed header and body
    :param header_dict: dictionary of header
    :param body_dict: dictionary of body
    :return:
    """
    request_dict = {"header": header_dict, "body": body_dict}
    request_msg = json.dumps(request_dict, indent=2)

    return request_msg


def insert_sorted(lst, item, maxlen, remove=0, comp=lambda x: x, eq=lambda x: x):
    """
    Inserts item into list in sorted order
    If element already exists, does not insert
    :param lst: list to insert into
    :param item: item to insert
    :param maxlen: maximum length of list
    :param remove: which item to remove if list is full after insertion (0 for first, -1 for last)
    :param comp: comparison function to use during search
    :param eq: equality function to use to determine if item already exists
    :return: None
    """
    if len(lst) == 0:
        lst.append(item)
        return

    insert_index = bisect_left(lst, comp(item), key=comp)

    if insert_index < len(lst) and eq(lst[insert_index]) == eq(item):
        return

    lst.insert(insert_index, item)

    if len(lst) > maxlen:
        lst.pop(remove)


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


def ask_peer(peer_addr, req_type, body_dict, custom_timeout=None):
    """
    Edited version of ask_peer for general use outside Node
    Sends a request and returns the response
    :param peer_addr: (IP, port) of peer
    :param req_type: type of request for request header
    :param body_dict: dictionary of body
    :param custom_timeout: timeout for request; network parameter is used if None
    :return: string response of peer
    """

    request_msg = create_request({"type": req_type}, body_dict)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client.settimeout(
            custom_timeout if custom_timeout is not None else params["net"]["timeout"]
        )
        try:
            client.connect(peer_addr)
            client.sendall(request_msg.encode())
            data = client.recv(params["net"]["data_size"]).decode()
        except (socket.error, socket.timeout):
            return None

    if not data:
        return None

    return json.loads(data)


class LRUCache:
    class Node:
        def __init__(self, key, value):
            self.key = key
            self.value = value
            self.prev = None
            self.next = None

        def __repr__(self):
            return str(self.value)

    def __init__(self, capacity):
        self.capacity = capacity
        self.dict = {}
        self.head = LRUCache.Node(0, 0)
        self.tail = LRUCache.Node(0, 0)
        self.head.next = self.tail
        self.tail.prev = self.head

    def _add_node(self, node):
        node.prev = self.head
        node.next = self.head.next
        self.head.next.prev = node
        self.head.next = node

    def _remove_node(self, node):
        prev = node.prev
        next = node.next
        prev.next = next
        next.prev = prev

    def _move_to_head(self, node):
        self._remove_node(node)
        self._add_node(node)

    def _pop_tail(self):
        res = self.tail.prev
        self._remove_node(res)
        return res

    def get(self, key):
        node = self.dict.get(key, None)
        if node is None:
            return None
        self._move_to_head(node)
        return node.value

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError
        return value

    def __setitem__(self, key, value):
        node = self.dict.get(key)
        if not node:
            newNode = LRUCache.Node(key, value)
            self.dict[key] = newNode
            self._add_node(newNode)
            if len(self.dict) > self.capacity:
                tail = self._pop_tail()
                del self.dict[tail.key]
        else:
            node.value = value
            self._move_to_head(node)

    def __contains__(self, key):
        return key in self.dict

    def __repr__(self):
        return str(self.dict)


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
