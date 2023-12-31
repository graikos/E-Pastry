import socket
import selectors
import time
import json
#project files
from src import utils
from src.utils import log

class ConnectionPool:
    """
    Defines a connection pool for the node
    The pool handles all network communication for the node
    The pool contains all currently open connections, incomning and outgoing
    """
    SocketErrors = (OSError, TimeoutError, ConnectionError)

    def __init__(self, port):
        self.SERVER_ADDR = ("", port) if port is not None else (utils.get_ip(), utils.params["host"]["server_port"])
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.server.bind(self.SERVER_ADDR)
        except ConnectionPool.SocketErrors:
            log.critical("Failed to bind server socket")
            exit(1)

        self.server.listen()
        self.server.setblocking(False)

        self.outgoing_connections = {}
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.server, selectors.EVENT_READ, self._accept_connection_cb)

    def cleanup_outgoing(self):
        """
        Removes all outgoing connections that have timed out
        :return: None
        """
        self.outgoing_connections = {key: value for key, value in self.outgoing_connections.items() if time.time() - value[1] < utils.params["net"]["connection_lifespan"]}

    def send(self, peer_addr, request_msg, pre_request, hold_connection=True):
        """
        Sends a request to a peer with the given address
        Returns the response from the peer
        :param peer_adr: address of peer
e       :param request_msg: request message to send
        :param pre_request: boolean indicating if pre-request with size should be sent
        :return: peer response, or False if failed
        """
        client = self._get_connection(peer_addr, hold_connection)
        if not client:
            log.info(f"Failed to connect to {peer_addr}")
            return False
        
        enc_request_msg = request_msg.encode()
        pre_req_msg = ""
        # pre_request will be sent as one message, along with the actual request
        # this is required due to the non-blocking setting on the receiving end
        if pre_request:
            pre_req_msg = utils.create_request({"type": "size"}, {"data_size": len(enc_request_msg)})
            # using $ as delimiter to identify pre-requests
            pre_req_msg = "$" + pre_req_msg + "$"

        success = self._send_safe(client, pre_req_msg.encode() + enc_request_msg, peer_addr, hold_connection)
        if not success:
            return False

        # catch timeout in case peer drops after receiving request
        try:
            data = client.recv(utils.params["net"]["data_size"]).decode()
        except ConnectionPool.SocketErrors as e:
            log.info(f"Error receiving from connection to {peer_addr}, got: {e}")
            return False
        
        return data
    
    def _send_safe(self, client, enc_request_msg, peer_addr, hold_connection):
        """
        Tries to send a request to a given peer
        If the connection fails, tries to get a new connection and send again
        :param client: the connection to send on
        :param enc_request_msg: the encoded request message to send
        :param peer_addr: the address of the peer
        :param hold_connection: boolean indicating if the connection should be held
        :return: boolean indicating if the request was sent successfully
        """
        try:
            client.sendall(enc_request_msg)
        except ConnectionPool.SocketErrors as e:
            del self.outgoing_connections[peer_addr]
            client = self._get_connection(peer_addr, hold_connection)
            if not client:
                log.info(f"Failed to connect to {peer_addr} while safe-sending, got: {e}")
                return False
            client.sendall(enc_request_msg)

        return True
        
    def _get_connection(self, addr, hold_connection):
        """
        Gets an (outgoing) connection to a peer
        If the connection is already open, it is returned and refreshed
        Otherwise, a new connection is opened
        :param addr: address of peer
        :param hold_connection: boolean indicating if the connection should be held
        :return: connection to peer, or False if failed
        """
        if addr in self.outgoing_connections:
            self.outgoing_connections[addr][1] = time.time()
            return self.outgoing_connections[addr][0]
        
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client.settimeout(utils.params["net"]["timeout"])
        try:
            client.connect(addr)
        except ConnectionPool.SocketErrors as e:
            log.info(f"Error connecting to new connection {addr}, got: {e}")
            return False
        
        if hold_connection:
            self.outgoing_connections[addr] = [client, time.time()]
        
        return client

    def select_incoming(self, handler):
        """
        Selects incoming connections
        :param handler: the handler to call on incoming connections
        :return: None
        """
        events = self.selector.select()
        for key, _ in events:
            key.data(key.fileobj, handler)
        
    def _accept_connection_cb(self, sock, handler=None):
        """
        Callback for accepting a new connection
        This is used to add connections to the pool that are initiated by other nodes
        :param sock: the socket to accept
        :param handler: the handler to call on the connection (unused, but required by select)
        :return: None
        """
        conn, addr = sock.accept()
        conn.setblocking(False)
        log.info(f"Accepted connection from {addr}")
        self.selector.register(conn, selectors.EVENT_READ, self._read_from_connection_cb)

    def _read_from_connection_cb(self, sock, handler):
        """
        Callback for reading from a connection
        :param sock: the socket to read from
        :param handler: the handler to call on the data
        :return: None
        """
        try:
            data = sock.recv(utils.params["net"]["data_size"])
        except ConnectionPool.SocketErrors as e:
            data = None

        if data:
            handler(sock, data.decode())
        else:
            log.info(f"Closing connection to {sock.getpeername()}")
            self.selector.unregister(sock)
            sock.close()

    def get_seed(self, node_id):
        """
        Gets the seed node from the seed server
        :param node_id: the node_id of the node
        :return: the seed node
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(utils.params["net"]["timeout"])
            for i in range(utils.params["seed_server"]["attempt_limit"]):
                try:
                    client.connect((utils.params["seed_server"]["ip"], utils.params["seed_server"]["port"]))
                    break
                except ConnectionPool.SocketErrors as e:
                    log.info(f"Failed to connect to seed server, retrying... "
                             f"{i + 1}/{utils.params['seed_server']['attempt_limit']}")
                    log.info(f"Got {e}")
                    time.sleep(2)
            else:
                log.critical("Connection to seed failed (attempt limit reached)")
                exit(1)
            client.sendall(utils.create_request({"type": "get_seed"},
                                                {"ip": self.SERVER_ADDR[0], "port": self.SERVER_ADDR[1],
                                                 "node_id": node_id}).encode())
            data = json.loads(client.recv(utils.params["net"]["data_size"]).decode())

        return data
