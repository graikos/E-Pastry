import json
import time
import threading
from random import randint
from math import ceil
from queue import Queue
from hashlib import sha1

# project files
from src.Storage import Storage
from src.ConnectionPool import ConnectionPool
from src.Link import Link
from src import utils
from src.utils import log
from src.rpc_handlers import REQUEST_MAP, STATUS_CONFLICT

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

        self.leaf_set_smaller = []
        self.leaf_set_greater = []

        self.neighborhood_set = []

        self.state_mutex = utils.RWLock()

        self.routing_table = [
            [None] * 2 ** utils.params["ring"]["b"]
            for _ in range(
                ceil(utils.params["ring"]["bits"] / utils.params["ring"]["b"])
            )
        ]

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
                        "route",
                        {
                            "node_id": self.node_id,
                            "ip": self.conn_pool.SERVER_ADDR[0],
                            "port": self.conn_pool.SERVER_ADDR[1],
                            "start_row": 0,
                            "initial": True,
                        },
                        extra_header={"message_type": "join"},
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
                    self.neighborhood_set = [
                        Link((n["ip"], n["port"]), n["node_id"])
                        for n in response["body"]["neighborhood_set"]
                    ]

                    # copy Z's leaf set into A's leaf set
                    self.leaf_set_smaller = [
                        Link((n["ip"], n["port"]), n["node_id"])
                        for n in response["body"]["leaf_set_smaller"]
                    ]

                    self.leaf_set_greater = [
                        Link((n["ip"], n["port"]), n["node_id"])
                        for n in response["body"]["leaf_set_greater"]
                    ]

                    # keep relevant nodes of leaf set based on node_id
                    if self.node_id > self.leaf_set_smaller[-1]:
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

                    # copy resulting routing table into A's routing table
                    for i, row in enumerate(response["body"]["routing_table"]):
                        for j, node in enumerate(row):
                            if node is not None:
                                self.routing_table[i][j] = Link(
                                    (node["ip"], node["port"]), node["node_id"]
                                )

                # if seed node is dead, reseed
                if seed_dead:
                    log.info("Seed is dead, retrying...")
                    continue

            # if this is the first node
            else:
                log.info("No other nodes in the network")

            break

        self_link = Link(self.conn_pool.SERVER_ADDR, self.node_id)
        for i, digit in enumerate(self.node_id_digits):
            self.routing_table[i][digit] = self_link

        # TODO: second join phase

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
        request_msg = utils.create_request(
            {"type": req_type, **extra_header}, body_dict
        )

        # if request is on this node, call RPC handler directly
        if peer_addr == self.conn_pool.SERVER_ADDR:
            data = REQUEST_MAP[req_type](self, body_dict)
        # else, make request for RPC
        else:
            data = self.conn_pool.send(
                peer_addr, request_msg, pre_request, hold_connection
            )

        if w_mode:
            self.state_mutex.w_enter()

        if not data:
            return None

        return json.loads(data)

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
