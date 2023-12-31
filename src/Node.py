import json
import time
import threading
from math import ceil
from queue import Queue
from hashlib import sha1

# project files
from src.Storage import Storage
from src.ConnectionPool import ConnectionPool
from src.Link import Link
from src import utils
from src.utils import log
from src.rpc_handlers import REQUEST_MAP

hash_func = sha1
Link.hash_func = hash_func


class Node:
    def __init__(self, port=None):
        self.storage = Storage()

        self.event_queue = Queue()

        self.leaf_set_smaller = []
        self.leaf_set_greater = []

        self.neighborhood_set = []

        self.state_mutex = utils.RWLock()

        self.routing_table = [[None] * 2 ** utils.params["ring"]["b"] for _ in range(ceil(utils.params["ring"]["bits"] / utils.params["ring"]["b"]))]

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
        """ """
        self_link = Link(self.conn_pool.SERVER_ADDR, self.node_id)
        for i, digit in enumerate(self.node_id_digits):
            self.routing_table[i][digit] = self_link

    def ask_peer(
        self, peer_addr, req_type, body_dict, pre_request=False, hold_connection=True
    ):
        """
        Makes request to peer, sending request_msg
        Releases writer lock if it is enabled, so RPCs can be handled while waiting for response
        Re-locks writer at the end of the method call if it was enabled
        :param peer_addr: (IP, port) of peer
        :param req_type: type of request for request header
        :param body_dict: dictionary of body
        :param pre_request: whether request should be preceded by request of type size
        :return: string response of peer
        """
        w_mode = self.state_mutex.w_locked()
        if w_mode:
            self.state_mutex.w_leave()

        request_msg = utils.create_request({"type": req_type}, body_dict)

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
