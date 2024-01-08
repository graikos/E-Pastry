import json
import time
import threading
from random import uniform
from math import ceil
from queue import Queue
from bisect import bisect_left

# project files
from src.Storage import Storage
from src.ConnectionPool import ConnectionPool
from src.Link import Link
from src import utils
from src.utils import log, hash_func
from src.rpc_handlers import (
    REQUEST_MAP,
    STATUS_CONFLICT,
    STATUS_UPDATE_REQUIRED,
    EXPECTED_REQUEST,
)

Link.hash_func = hash_func


class Node:
    def __init__(self, port=None):
        # random location for node
        # in a real implementation, this would be based on the node's IP
        self.latitude = uniform(-90, 90)
        self.longitude = uniform(-180, 180)

        self.storage = Storage()

        self.event_queue = Queue()

        # dictionary mapping node_ids to distances to nodes
        self.id_to_distance = utils.LRUCache(utils.params["node"]["cache_size"])

        self.leaf_set_smaller = []
        self.leaf_set_greater = []

        self.neighborhood_set = []
        self.current_neighbor = 0  # used for polling

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
        self.node_id_digits = list(utils.get_id_digits(self.node_id))
        utils.logging.basicConfig(
            format=f"%(threadName)s:{self.node_id}-%(levelname)s: %(message)s",
            level=utils.environ.get("LOGLEVEL", utils.params["logging"]["level"]),
        )
        self.self_link = Link(self.conn_pool.SERVER_ADDR, self.node_id)
        log.debug(f"Initialized with node ID: {self.node_id}")
        self.join_ring()

        self.ask_peer(
            (utils.params["seed_server"]["ip"], utils.params["seed_server"]["port"]),
            "add_node",
            {
                "ip": self.conn_pool.SERVER_ADDR[0],
                "port": self.conn_pool.SERVER_ADDR[1],
                "node_id": self.node_id,
                "latitude": self.latitude,
                "longitude": self.longitude,
            },
            hold_connection=False,
        )

        self.listen()

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
        response = None
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
                            "route_ids": [],
                        },
                    )

                    log.debug(f"Response: {response}")

                    if not response:
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

                    if response["header"]["status"] not in range(200, 300):
                        log.debug("Failed to join, trying again...")
                        break

                    # copy Z's leaf set into A's leaf set
                    self._update_leaf_set(
                        response["body"]["leaf_set_smaller"],
                        response["body"]["leaf_set_greater"],
                    )

                    # initialize state from response
                    # set neighborhood set as A's neighborhood set
                    self._update_neighborhood_set(response["body"]["neighborhood_set"])

                    # copy resulting routing table into A's routing table
                    self._update_routing_table(response["body"]["routing_table"])

                    log.info("Updated state")
                    break

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
                if str(link.node_id) in response["body"]["node_info"]:
                    extra_header = {"in_route": True}
                    req_body = {
                        "node_id": self.node_id,
                        "ip": self.conn_pool.SERVER_ADDR[0],
                        "port": self.conn_pool.SERVER_ADDR[1],
                        **response["body"]["node_info"][str(link.node_id)],
                    }
                else:
                    extra_header = {"in_route": False}
                    req_body = {
                        "node_id": self.node_id,
                        "ip": self.conn_pool.SERVER_ADDR[0],
                        "port": self.conn_pool.SERVER_ADDR[1],
                    }

                response2 = self.ask_peer(
                    link.addr,
                    "just_joined",
                    req_body,
                    extra_header=extra_header,
                    pre_request=False,
                )
                request_addresses.add(link.addr)
                if not response2:
                    # TODO: handle dead node
                    continue
                if response2["header"]["status"] == STATUS_UPDATE_REQUIRED:
                    if response["body"]["node_info"][str(link.node_id)]["is_last"]:
                        self._update_leaf_set(
                            response2["body"]["leaf_set_smaller"],
                            response2["body"]["leaf_set_greater"],
                        )

                    if response["body"]["node_info"][str(link.node_id)]["is_first"]:
                        self._update_neighborhood_set(
                            response2["body"]["neighborhood_set"]
                        )

                    self._update_routing_table(
                        response2["body"]["routing_table"],
                        response["body"]["node_info"][str(link.node_id)]["start_row"],
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
            self.id_to_distance[link.node_id] = self.get_distance_cached(
                link.node_id, link.addr
            )
            self.insert_into_leaf_set(link)

        # sort neighborhood set by distance
        self.neighborhood_set.sort(
            key=lambda x: self.get_distance_cached(x.node_id, x.addr)
        )

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
        if (
            not self.leaf_set_smaller
            or self.node_id > self.leaf_set_smaller[-1].node_id
        ):
            for i, link in enumerate(self.leaf_set_greater):
                if self.node_id < link.node_id:
                    self.leaf_set_smaller.extend(self.leaf_set_greater[:i])
                    self.leaf_set_smaller = self.leaf_set_smaller[
                        -utils.params["node"]["L"] // 2 :
                    ]
                    self.leaf_set_greater = self.leaf_set_greater[i:]
                    break
            else:
                self.leaf_set_smaller.extend(self.leaf_set_greater)
                self.leaf_set_smaller = self.leaf_set_smaller[
                    -utils.params["node"]["L"] // 2 :
                ]
                self.leaf_set_greater = []
        else:
            for i, link in reversed(list(enumerate(self.leaf_set_smaller))):
                if self.node_id > link.node_id:
                    self.leaf_set_greater[0:0] = self.leaf_set_smaller[i + 1 :]
                    self.leaf_set_greater = self.leaf_set_greater[
                        : utils.params["node"]["L"] // 2
                    ]
                    self.leaf_set_smaller = self.leaf_set_smaller[: i + 1]
                    break
            else:
                self.leaf_set_greater[0:0] = self.leaf_set_smaller
                self.leaf_set_greater = self.leaf_set_greater[
                    : utils.params["node"]["L"] // 2
                ]
                self.leaf_set_smaller = []

    def insert_into_leaf_set(self, node_link):
        """
        Inserts a link into the appropriate leaf set, if it doesn't already exist
        :param node_link: link to insert
        :return: None
        """
        if node_link.node_id < self.node_id:
            utils.insert_sorted(
                self.leaf_set_smaller,
                node_link,
                utils.params["node"]["L"] // 2,
                remove=0,
                comp=lambda x: x.node_id,
                eq=lambda x: x.node_id,
            )
        else:
            utils.insert_sorted(
                self.leaf_set_greater,
                node_link,
                utils.params["node"]["L"] // 2,
                remove=-1,
                comp=lambda x: x.node_id,
                eq=lambda x: x.node_id,
            )

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
                    self.insert_into_leaf_set(self.routing_table[i + start_index][j])

    def find_key(self, key):
        """
        Finds node that contains key and returns the key's value
        :param key: key to find
        :return: value of key, or None if key is not found
        """
        new_node = self.locate_closest(key)
        if new_node is None:
            return None

        response = self.ask_peer(
            (new_node["ip"], new_node["port"]), "lookup", {"key": key}
        )

        if not response or response["header"]["status"] not in range(200, 300):
            return None

        return response["body"]["value"]

    def find_and_store_key(self, key, value):
        """
        Finds node that key should be stored in and stores it there
        If the key already exists, this will update its value with the given value
        :param key: the key
        :param value: the value of the key
        :return: bool, whether the insertion was successful
        """
        new_node = self.locate_closest(key)

        if new_node is None:
            log.debug("Could not find node")
            return False

        response = self.ask_peer(
            (new_node["ip"], new_node["port"]),
            "store_key",
            {"key": key, "value": value},
        )

        if not response or response["header"]["status"] not in range(200, 300):
            log.debug("Could not store key")
            return False

        log.debug("Stored pair")
        return True

    def find_and_delete_key(self, key):
        """
        Finds node that key should be deleted from and deletes it there
        :param key: the key
        :return: bool, whether the deletion was successful
        """
        new_node = self.locate_closest(key)

        if new_node is None:
            log.debug("Could not find node")
            return False

        response = self.ask_peer(
            (new_node["ip"], new_node["port"]), "delete_key", {"key": key}
        )

        if not response or response["header"]["status"] not in range(200, 300):
            log.debug("Could not delete key")
            return False

        log.debug("Deleted pair")
        return True

    def locate_closest(self, key):
        """
        Locates node that is responsible for key and returns it
        :param key: key to find
        :return: address and id of node that is responsible for key, or None if key is not found
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

    def handle_join(self, id, start_row, route_ids):
        """
        Handles join request
        :param id: id of node that is joining
        :param start_row: row to start routing table at
        :param route_ids: list of ids of nodes that are in route
        :return: tuple of (
            string of response,
            whether this is the last node,
            whether to append routing table rows
        ), or None if request fails
        """
        next_link = self.route(id, is_id=True)

        if next_link is None:
            return None
        elif next_link is self.self_link:
            log.debug("Last node in route, sending leaf set")
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
                True,  # value used to inform rpc handler that routing table rows should be appended
            )

        key_digits = list(utils.get_id_digits(id))
        pass_rows = utils.get_longest_common_prefix(
            self.node_id_digits, key_digits
        ) < utils.get_longest_common_prefix(
            list(utils.get_id_digits(next_link.node_id)), key_digits
        )

        response = self.ask_peer(
            next_link.addr,
            "join",
            {
                "node_id": id,
                "start_row": start_row,
                "initial": False,
                "route_ids": route_ids + [self.node_id],
            },
        )
        if not response:
            return None

        if response["header"]["status"] == STATUS_CONFLICT:
            log.debug("Conflict in route, sending leaf set")
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
                True,  # value used to inform rpc handler that routing table rows should be appended
            )

        if response["header"]["status"] not in range(200, 300):
            return None

        return (response["body"], False, pass_rows)

    def route(self, key, is_id=False):
        """
        Finds node that is responsible for key and returns it
        :param key: key to find
        :param is_id: whether key is already an id
        :return: value of key, or None if key is not found
        """
        key_id = utils.get_id(key, hash_func) if not is_id else key

        leaf_set = self.leaf_set_smaller + [self.self_link] + self.leaf_set_greater
        # if key is in leaf set, return closest node
        if leaf_set[0].node_id <= key_id <= leaf_set[-1].node_id:
            closest_index = bisect_left(leaf_set, key_id, key=lambda x: x.node_id)
            if leaf_set[closest_index].node_id == key_id:
                node_link = leaf_set[closest_index]
            else:
                log.debug(f"Leaf set is {leaf_set}")
                log.debug(
                    f"Comparing {leaf_set[closest_index]} and {leaf_set[closest_index - 1]}"
                )
                node_link = utils.get_numerically_closest(
                    leaf_set[closest_index], leaf_set[closest_index - 1], key_id
                )
                log.debug(f"Closest node: {node_link}")
        # else, find closest node in routing table
        else:
            key_digits = list(utils.get_id_digits(key_id))
            l = utils.get_longest_common_prefix(self.node_id_digits, key_digits)
            node_link = self.routing_table[l][key_digits[l]]
            if node_link is None:
                # rare case where node is not in routing table
                node_link = self.self_link
                distance = abs(self.node_id - key_id)
                for link in (
                    self.neighborhood_set
                    + leaf_set
                    + [item for row in self.routing_table[l:] for item in row]
                ):
                    if (
                        link is not None
                        and utils.get_longest_common_prefix(
                            self.node_id_digits, utils.get_id_digits(link.node_id)
                        )
                        >= l
                    ):
                        link_distance = abs(link.node_id - key_id)
                        if link_distance < distance:
                            node_link = link
                            distance = link_distance

        return node_link

    def poll_neighbor(self):
        """
        Polls a neighbor node
        If it is dead, asks other neighbors for their neighborhood sets
        and keeps the closest node that is not already in the neighborhood set
        :return: None
        """
        if len(self.neighborhood_set) == 0:
            return

        neighbor = self.neighborhood_set[self.current_neighbor]
        response = self.ask_peer(neighbor.addr, "poll", {}, output=False)

        if not response:
            self.neighborhood_set.remove(neighbor)
            if neighbor.node_id in self.id_to_distance:
                del self.id_to_distance[neighbor.node_id]

            current_neighbors = set(x.node_id for x in self.neighborhood_set)
            other_neighbors = set()
            for link in self.neighborhood_set:
                response = self.ask_peer(link.addr, "get_neighborhood_set", {})
                if not response:
                    continue

                other_neighbors |= set(
                    (
                        Link((n["ip"], n["port"]), n["node_id"])
                        for n in response["body"]["neighborhood_set"]
                    )
                )

            distance = float("inf")
            closest_neighbor = None
            for neighbor in other_neighbors:
                if neighbor.node_id not in current_neighbors:
                    if neighbor.node_id in self.id_to_distance:
                        dist = self.id_to_distance[neighbor.node_id]
                    else:
                        dist = self.get_distance_to(neighbor.addr)
                        if dist is None:
                            continue

                    if dist < distance:
                        distance = dist
                        closest_neighbor = neighbor

            if closest_neighbor is not None:
                utils.insert_sorted(
                    self.neighborhood_set,
                    closest_neighbor,
                    utils.params["node"]["M"],
                    remove=-1,
                    comp=lambda x: self.get_distance_cached(x.node_id, x.addr),
                    eq=lambda x: x.node_id,
                )

        self.current_neighbor = (self.current_neighbor + 1) % len(self.neighborhood_set)

    def ask_peer(
        self,
        peer_addr,
        req_type,
        body_dict,
        extra_header={},
        pre_request=False,
        hold_connection=True,
        output=True,
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
        :param hold_connection: whether connection should be held open after request
        :param output: whether to output debug messages
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
                peer_addr, request_msg, pre_request, hold_connection, output
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
            distance = self.get_distance_to(node_addr)
            if distance is None:
                # TODO: handle dead node
                pass
            self.id_to_distance[node_id] = distance

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

    def listen(self):
        """
        Main server loop
        Starts a thread to accept connections using the connection pool
        Starts a thread to handle periodic events
        Loop is considered main thread/writer: reads shared queue and executes calls
        :return: None
        """

        log.info(
            f"Starting node on {self.conn_pool.SERVER_ADDR[0]}:{self.conn_pool.SERVER_ADDR[1]}"
        )

        # accept incoming connections
        connection_listener = threading.Thread(target=self.handle_connections)
        connection_listener.name = "Connection Listener"
        connection_listener.daemon = True

        # start timer to handle periodic events
        timer = threading.Thread(
            target=self.polling_timer,
            args=(self.event_queue, utils.params["net"]["polling_interval"]),
        )
        timer.name = "Timer"
        timer.daemon = True

        connection_listener.start()
        timer.start()

        while True:
            # wait until event_queue is not empty, then pop
            data = self.event_queue.get()

            self.state_mutex.w_enter()

            if data == 0:
                self.poll_neighbor()
            elif callable(data):
                data()

            self.state_mutex.w_leave()

    def handle_connections(self):
        """
        Handles incoming connections
        :return: None
        """
        while True:
            self.conn_pool.select_incoming(
                lambda conn, addr: self.handle_response(conn, addr)
            )

    def handle_response(self, connection, data):
        """
        Handler function to be called when a message is received
        Passed as lambda to ConnectionPool, including the node object
        :param connection: the connection object (passed by ConnectionPool)
        :param data: the data received (passed by ConnectionPool)
        """
        # if $ is first character, pre_request is contained
        if data[0] == "$":
            # split to ['', pre_request, main_request]
            data = data.split("$")
            # pre-request is the first part of the received data
            pre_request = data[1]
            pre_request = json.loads(pre_request)

            data_size = pre_request["body"]["data_size"]

            # anything received after is part of the main request
            main_request = "".join(data[2:])
            size_received = len(main_request.encode())

            connection.setblocking(True)
            # data might be large chunk, so read in batches
            while size_received < data_size:
                next_data = connection.recv(utils.params["net"]["data_size"])
                size_received += len(next_data)
                main_request += next_data.decode()

            connection.setblocking(False)

            data = main_request

        data = json.loads(data)

        # ensure request type exists
        if data["header"]["type"] not in EXPECTED_REQUEST:
            log.debug(f"Got RPC call of unknown type: {data['header']['type']}")
            return

        # ensure all expected arguments have been sent
        exp_req = EXPECTED_REQUEST[data["header"]["type"]]
        if len(exp_req) == 3 and type(exp_req[0]) == str and type(exp_req[1]) == tuple:
            extra_header = exp_req[0]
            if extra_header not in data["header"]:
                log.debug(
                    f"RPC call of type {data['header']['type']} missing required header {extra_header}"
                )
                return
            exp_req = exp_req[1 if data["header"][extra_header] else 2]
            data["body"][extra_header] = data["header"][extra_header]

        for arg in exp_req:
            if arg not in data["body"]:
                log.debug(f"RPC call of type {data['header']['type']} missing {arg}")
                return

        # TODO: remove
        if data["header"]["type"] != "poll":
            log.debug(f"Got RPC call of type: {data['header']['type']}")

        # get mutex so main thread doesn't change object data during RPC
        self.state_mutex.r_enter()
        # select RPC handler according to RPC type
        response = REQUEST_MAP[data["header"]["type"]](self, data["body"])
        self.state_mutex.r_leave()

        connection.sendall(response.encode())

    @staticmethod
    def polling_timer(event_queue, delay):
        """
        Sleeps for specified amount of time, then places 0 in queue
        :param event_queue: shared queue
        :param delay: amount of time to sleep for
        :return: None
        """
        while True:
            time.sleep(delay)
            event_queue.put(0)
