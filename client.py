import readline
from src import utils
from sys import argv

HELP_MSG = (
    "Available commands:\n"
    ">lookup [key]\n"
    ">insert [key] [value]\n"
    ">delete [key]\n"
    ">leave"
)

command_help = {
    "lookup": "Usage: lookup [key]\n" "Finds value of given key in E-Chord.",
    "insert": "Usage: insert [key] [value]\n" "Inserts pair (key, value) into E-Chord.",
    "delete": "Usage: delete [key]\n" "Deletes key from E-Chord.",
    "leave": "Usage: leave\n" "Tells node to leave ring",
    "debug": "Usage: debug [type]\n",
    "help": HELP_MSG,
}


def run_client(peer_addr):
    while True:
        command = input(">")
        command = command.lower().split(" ")

        if command[0] == "lookup":
            if len(command) != 2:
                print(command_help["lookup"])
                continue

            response = utils.ask_peer(peer_addr, "find_key", {"key": command[1]})

            if not response or response["header"]["status"] not in range(200, 300):
                print("Key not found.")
                continue

            print(f"Key {command[1]} has value: {response['body']['value']}")

        elif command[0] == "insert":
            if len(command) != 3:
                print(command_help["insert"])
                continue

            response = utils.ask_peer(
                peer_addr,
                "find_and_store_key",
                {"key": command[1], "value": command[2]},
            )

            if not response or response["header"]["status"] not in range(200, 300):
                print("Could not store key.")
                continue

            print("Successfully stored pair.")

        elif command[0] == "delete":
            if len(command) != 2:
                print(command_help["delete"])
                continue

            response = utils.ask_peer(
                peer_addr, "find_and_delete_key", {"key": command[1]}
            )

            if not response or response["header"]["status"] not in range(200, 300):
                print("Could not delete key.")
                continue

            print("Successfully deleted key.")

        elif command[0] == "leave":
            if len(command) != 1:
                print(command_help["leave"])
                continue

            utils.ask_peer(peer_addr, "leave_ring", {})
            break

        elif command[0] == "debug":
            if len(command) != 2:
                print(command_help["debug"])
                continue

            if command[1] == "state":
                response = utils.ask_peer(peer_addr, "debug_state", {})
            else:
                print("Unknown debug type")

        elif command[0] == "help":
            print(command_help["help"])

        elif command[0] == "exit":
            return

        else:
            print("Unknown command. Type help for a list of commands.")


if __name__ == "__main__":
    if len(argv) != 3:
        print("Expected arguments: IP, PORT")
        exit(1)
    run_client((argv[1], int(argv[2])))
