import json
import time
import threading
import heapq
from random import randint
from math import ceil
from queue import Queue
from hashlib import sha1
from bisect import bisect_left

# project files
from src.Storage import Storage
from src.ConnectionPool import ConnectionPool
from src.Link import Link
from src import utils
from src.utils import log
from src.rpc_handlers import REQUEST_MAP, STATUS_CONFLICT, STATUS_UPDATE_REQUIRED

hash_func = sha1
Link.hash_func = hash_func


class Node:
    def __init__(self, port=None):
        # random location for node
        # in a real implementation, this would be based on the node's IP
        self.latitude = randint(-90, 90)
        self.longitude = randint(-180, 180)

        self.storage = Storage()

        self.event_queue = Queue()

        # dictionary mapping node_ids to distances to nodes
        self.id_to_distance = utils.LRUCache(utils.params["node"]["cache_size"])

        self.leaf_set_smaller = []
        self.leaf_set_greater = []

        self.neighborhood_set = []

        self.routing_table = [
            [None] * 2 ** utils.params["ring"]["b"]
            for _ in range(
                ceil(utils.params["ring"]["bits"] / utils.params["ring"]["b"])
            )
        ]

        self.update_timestamp = 0

        self.state_mutex = utils.RWLock()

        self.conn_pool = ConnectionPool(port)

        self.node_id = utils.get_id(
            self.conn_pool.SERVER_ADDR[0] + str(self.conn_pool.SERVER_ADDR[1]),
            hash_func,
        )
        self.node_id_digits = utils.get_id_digits(self.node_id)
        utils.logging.basicConfig(
            format=f"%(threadName)s:{self.node_id}-%(levelname)s: %(message)s",
            level=utils.environ.get("LOGLEVEL", utils.params["logging"]["level"]),
        )
        self.self_link = Link(self.conn_pool.SERVER_ADDR, self.node_id)
        log.debug(f"Initialized with node ID: {self.node_id}")
        self.join_ring()

    def join_ring(self):
        """
        Gets a seed node from the seed server and joins the ring
        The join process consists of two phases:
            1. Send a special join message to the seed node
            The seed node A routes the message to the closest node Z
            Each node on the route extends the routing table with the requires row(s)
            Node A appends its neighborhood set, while Z appends its leaf set
            The final response contains the resulting sets and the routing table
        """
        while True:
            # get initial node from seed server
            data = self.conn_pool.get_seed(
                self.node_id, (self.latitude, self.longitude)
            )
            log.debug("Asked seed server")
            if data["header"]["status"] == STATUS_CONFLICT:
                log.critical("ID conflict in network. Please change port.")
                exit(1)

            # Join ring
            # if at least one other node exists
            if data["header"]["status"] in range(200, 300):
                log.info("Got seed from seed server")
                log.debug(f"Seed address: {data['body']['ip'], data['body']['port']}")
                seed_dead = False
                while True:
                    log.info("Sending join request...")
                    response = self.ask_peer(
                        (data["body"]["ip"], data["body"]["port"]),
                        "join",
                        {
                            "node_id": self.node_id,
                            "start_row": 0,
                            "initial": True,
                        },
                    )

                    if not response or response["header"]["status"] not in range(
                        200, 300
                    ):
                        # tell seed server that seed node has died
                        self.ask_peer(
                            (
                                utils.params["seed_server"]["ip"],
                                utils.params["seed_server"]["port"],
                            ),
                            "dead_node",
                            {
                                "ip": data["body"]["ip"],
                                "port": data["body"]["port"],
                                "node_id": data["body"]["node_id"],
                            },
                            hold_connection=False,
                        )
                        seed_dead = True
                        break

                    # initialize state from response
                    # set neighborhood set as A's neighborhood set
                    self._update_neighborhood_set(response["body"]["neighborhood_set"])

                    # copy Z's leaf set into A's leaf set
                    self._update_leaf_set(
                        response["body"]["leaf_set_smaller"],
                        response["body"]["leaf_set_greater"],
                    )

                    # copy resulting routing table into A's routing table
                    self._update_routing_table(response["body"]["routing_table"])

                # if seed node is dead, reseed
                if seed_dead:
                    log.info("Seed is dead, retrying...")
                    continue

            # if this is the first node
            else:
                log.info("No other nodes in the network")

            break

        for i, digit in enumerate(self.node_id_digits):
            self.routing_table[i][digit] = self.self_link

        self.update_timestamp = 1

        request_addresses = set((self.conn_pool.SERVER_ADDR,))

        for link in (
            self.neighborhood_set
            + self.leaf_set_smaller
            + self.leaf_set_greater
            + [item for row in self.routing_table for item in row]
        ):
            if link is not None and link.addr not in request_addresses:
                response2 = self.ask_peer(
                    link.addr,
                    "just_joined",
                    {
                        "node_id": self.node_id,
                        "ip": self.conn_pool.SERVER_ADDR[0],
                        "port": self.conn_pool.SERVER_ADDR[1],
                        **response["body"]["node_info"][link.node_id],
                    },
                    pre_request=False,
                )
                request_addresses.add(link.addr)
                if not response2:
                    # TODO: handle dead node
                    continue
                if response2["header"]["status"] == STATUS_UPDATE_REQUIRED:
                    if response["body"]["node_info"][link.node_id]["is_first"]:
                        self._update_neighborhood_set(
                            response2["body"]["neighborhood_set"]
                        )

                    if response["body"]["node_info"][link.node_id]["is_last"]:
                        self._update_leaf_set(
                            response2["body"]["leaf_set_smaller"],
                            response2["body"]["leaf_set_greater"],
                        )

                    self._update_routing_table(
                        response2["body"]["routing_table"],
                        response["body"]["node_info"][link.node_id]["start_row"],
                    )

    def _update_neighborhood_set(self, response_set):
        """
        Updates neighborhood set with set from response
        :param response_set: set of nodes from response
        :return: None
        """
        self.neighborhood_set = [
            Link((n["ip"], n["port"]), n["node_id"]) for n in response_set
        ]

        for link in self.neighborhood_set:
            self.id_to_distance[link.node_id] = self.get_distance_cached(link.addr)

        # sort neighborhood set by distance
        self.neighborhood_set.sort(key=lambda x: x.distance)

    def _update_leaf_set(self, response_set_smaller, response_set_greater):
        """
        Updates leaf set with sets from response
        :param response_set_smaller: set of nodes from response
        :param response_set_greater: set of nodes from response
        :return: None
        """
        self.leaf_set_smaller = [
            Link((n["ip"], n["port"]), n["node_id"]) for n in response_set_smaller
        ]

        self.leaf_set_greater = [
            Link((n["ip"], n["port"]), n["node_id"]) for n in response_set_greater
        ]

        # keep relevant nodes of leaf set based on node_id
        if self.node_id > self.leaf_set_smaller[-1].node_id:
            for i, link in enumerate(self.leaf_set_greater):
                if self.node_id < link.node_id:
                    self.leaf_set_smaller.extend(self.leaf_set_greater[:i])
                    self.leaf_set_smaller = self.leaf_set_smaller[
                        -utils.params["node"]["L"] // 2 :
                    ]
                    self.leaf_set_greater = self.leaf_set_greater[i:]
                    break
        else:
            for i, link in reversed(list(enumerate(self.leaf_set_smaller))):
                if self.node_id > link.node_id:
                    self.leaf_set_greater[0:0] = self.leaf_set_smaller[-i:]
                    self.leaf_set_greater = self.leaf_set_greater[
                        : utils.params["node"]["L"] // 2
                    ]
                    self.leaf_set_smaller = self.leaf_set_smaller[:-i]
                    break

    def _update_routing_table(self, response_table, start_index=0):
        """
        Updates routing table with table from response
        :param response_table: table of nodes from response
        :return: None
        """
        for i, row in enumerate(response_table):
            for j, node in enumerate(row):
                if node is not None:
                    self.routing_table[i + start_index][j] = Link(
                        (node["ip"], node["port"]), node["node_id"]
                    )

    def locate_closest(self, key):
        """
        Locates node that is responsible for key and returns it
        :param key: key to find
        :return: address and id of node that is responsible for key
        """
        next_link = self.route(key)
        if next_link is None:
            return None
        elif next_link is self.self_link:
            return {
                "ip": self.conn_pool.SERVER_ADDR[0],
                "port": self.conn_pool.SERVER_ADDR[1],
                "node_id": self.node_id,
            }

        response = self.ask_peer(next_link.addr, "locate_closest", {"key": key})
        if not response or response["header"]["status"] not in range(200, 300):
            return None

        return response["body"]

    def handle_join(self, id, start_row):
        """
        Handles join request
        :param id: id of node that is joining
        :param start_row: row to start routing table at
        :return: tuple of (string of response, whether this is the last node)
        """
        next_link = self.route(id)
        if next_link is None:
            return None
        elif next_link is self.self_link:
            return (
                {
                    "leaf_set_smaller": [
                        {"ip": n.addr[0], "port": n.addr[1], "node_id": n.node_id}
                        for n in self.leaf_set_smaller
                    ],
                    "leaf_set_greater": [
                        {"ip": n.addr[0], "port": n.addr[1], "node_id": n.node_id}
                        for n in self.leaf_set_greater
                    ],
                },
                True,  # value used to inform rpc handler that this is the last node
            )

        response = self.ask_peer(
            next_link.addr,
            "join",
            {"id": id, "start_row": start_row, "initial": False},
        )
        if not response or response["header"]["status"] not in range(200, 300):
            return None

        return (response["body"], False)

    def route(self, key):
        """
        Finds node that is responsible for key and returns it
        :param key: key to find
        :return: value of key
        """
        key_id = utils.get_id(key, hash_func)

        leaf_set = self.leaf_set_smaller + [self.self_link] + self.leaf_set_greater
        # if key is in leaf set, return closest node
        if leaf_set[0].node_id <= key <= leaf_set[-1].node_id:
            closest_index = bisect_left(leaf_set, key_id)
            if leaf_set[closest_index].node_id == key_id:
                node_link = leaf_set[closest_index]
            else:
                node_link = utils.get_numerically_closest(
                    leaf_set[closest_index], leaf_set[closest_index - 1], key_id
                )
        # else, find closest node in routing table
        else:
            key_digits = list(utils.get_id_digits(key_id))
            l = utils.get_longest_common_prefix(self.node_id_digits, key_digits)
            node_link = self.routing_table[l][key_digits[l]]
            if node_link is None:
                # rare case where node is not in routing table
                min_heap = []
                for link in (
                    self.neighborhood_set
                    + leaf_set
                    + [item for row in self.routing_table[l:] for item in row]
                ):
                    if link is not None:
                        heapq.heappush(
                            min_heap,
                            (utils.get_ring_distance(link.node_id, key_id), link),
                        )
                # self is included in this list, so if self is returned, it means that no other node is closer
                node_link = heapq.heappop(min_heap)[1]
                # if no known node is closer, routing fails
                if node_link is self.self_link:
                    return None

        return node_link

    def ask_peer(
        self,
        peer_addr,
        req_type,
        body_dict,
        extra_header={},
        pre_request=False,
        hold_connection=True,
    ):
        """
        Makes request to peer, sending request_msg
        Releases writer lock if it is enabled, so RPCs can be handled while waiting for response
        Re-locks writer at the end of the method call if it was enabled
        :param peer_addr: (IP, port) of peer
        :param req_type: type of request for request header
        :param body_dict: dictionary of body
        :param extra_header: dictionary of extra header fields
        :param pre_request: whether request should be preceded by request of type size
        :return: string response of peer
        """
        w_mode = self.state_mutex.w_locked()
        if w_mode:
            self.state_mutex.w_leave()

        # if request is on this node, call RPC handler directly
        if peer_addr == self.conn_pool.SERVER_ADDR:
            data = REQUEST_MAP[req_type](self, body_dict)
        # else, make request for RPC
        else:
            request_msg = utils.create_request(
                {"type": req_type, **extra_header}, body_dict
            )
            data = self.conn_pool.send(
                peer_addr, request_msg, pre_request, hold_connection
            )

        if w_mode:
            self.state_mutex.w_enter()

        if not data:
            return None

        return json.loads(data)

    def get_distance_cached(self, node_id, node_addr):
        """
        Returns the distance from this node to the given node
        If the distance is not cached, it is calculated and cached
        :param node_id: ID of node
        :param node_addr: (IP, port) of node
        :return: distance
        """
        if node_id not in self.id_to_distance:
            self.id_to_distance[node_id] = self.get_distance_to(node_addr)

        return self.id_to_distance[node_id]

    def get_distance_to(self, node_addr):
        """
        Returns the distance from this node to the given node
        :param node_addr: (IP, port) of node
        :return: distance
        NOTE: This is a sample function that the protocol requires
        It is used to determine the real distance between two nodes
        For the local implementation, we use random coordinates for each node and calculate distance based on that
        In a real implementation, this would need to be changed to use the actual distance between the two nodes
        """
        response = self.ask_peer(node_addr, "get_coordinates", {})
        if not response or response["header"]["status"] not in range(200, 300):
            return None

        return utils.haversine(
            (self.latitude, self.longitude),
            (response["body"]["latitude"], response["body"]["longitude"]),
        )
