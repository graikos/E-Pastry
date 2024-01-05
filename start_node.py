from src.Node import Node
from sys import argv

if __name__ == "__main__":
    if len(argv) == 2:
        Node(int(argv[1]))
    else:
        Node()
