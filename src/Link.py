from src import utils


class Link:
    hash_func = None

    def __init__(self, addr, node_id=None):
        self.addr = addr
        if not node_id:
            self.node_id = utils.get_id(addr[0] + str(addr[1]), Link.hash_func)
        else:
            self.node_id = node_id

    def __eq__(self, other):
        return self.addr == other.addr

    def __hash__(self):
        return hash(self.addr)

    def __repr__(self):
        # return str(self.addr)
        return str(self.node_id)
