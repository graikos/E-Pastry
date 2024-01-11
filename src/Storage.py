class Storage:
    """
    Storage class
    Used to store (key, value) pairs on a node
    """

    def __init__(self):
        """
        Initializes a storage object
        """
        self.keys_to_ids = {}
        self.keys_to_vals = {}

    def get_id(self, key):
        """
        Returns the ID of the key in dictionary (if it exists)
        Raises KeyError if it doesn't
        :param key: the key
        :return: the ID
        """
        return self.keys_to_ids[key]

    def __getitem__(self, key):
        """
        Returns the value of the key in dictionary (if it exists)
        Raises KeyError if it doesn't
        :param key: the key
        :return: the value
        """
        return self.keys_to_vals[key]

    def add_key(self, key, value, key_id):
        """
        Adds a new key to storage
        :param key: the key
        :param value: the value
        :param key_id: the ID of the key
        :return: None
        """
        self.keys_to_ids[key] = key_id
        self.keys_to_vals[key] = value

    def __delitem__(self, key):
        """
        Removes a key (if it exists) from storage
        :param key: the key
        :return: None
        """

        if key in self.keys_to_ids:
            del self.keys_to_ids[key]
        if key in self.keys_to_vals:
            del self.keys_to_vals[key]

    def __contains__(self, key):
        """
        Returns true or false, depending on whether the key exists in storage
        :param key: the key to check for
        :return: bool
        """
        return key in self.keys_to_vals

    def __iter__(self):
        """
        Overloads iteration for the storage object
        :return: iterator for storage keys
        """
        return iter(self.keys_to_vals)

    def __str__(self):
        """
        Returns string representation of storage dictionary
        :return: the string representation
        """
        s = "{"
        for key in self:
            s += str(key) + ": " + f"(ID: {self.get_id(key)}, value: {self[key]}), "

        s = (s[:-2] if len(s) > 1 else s) + "}"

        return s

    def __len__(self):
        return len(self.keys_to_ids)

    def dump(self):
        """
        Dumps all storage data into a list of dictionaries, each having three keys: key, value, key_id with their
        corresponding values
        :return: the list of dicts
        """
        d = []
        for key in self:
            d.append({"key": key, "value": self[key], "key_id": self.get_id(key)})

        return d

    def dump_keys(self):
        """
        Dumps alll storage keys into a list
        :return: the list of keys
        """
        return list(self.keys_to_vals.keys())
