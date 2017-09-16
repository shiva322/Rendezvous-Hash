"""
You must use hashlib to hash the node ('0.0.0.0:3000') and key ('mykey') combination.
Example: 
x = '0.0.0.0:3000' + 'my-key'
x = node + key
hash = hashlib.md5(x).hexdigest()

More@https://docs.python.org/2/library/hashlib.html
"""
# TODO: Add any required import
import hashlib
from client import DBClient

class RendezvousHash(object):
    """
    This class implements the Rendezvous (HRW) hashing logic.
    DO NOT USE ANY STATIC CLASS VARIABLES!
    """

    def __init__(self, nodes=None):
        """
        Initialize an instance with a node list and others.
        A node means a server host name and its listening port. E.g. '0.0.0.0:3000' 
        :param nodes: a list of DB server nodes to register.
        """
        # TODO
        if nodes is None:
            nodes = []
        self.nodes = nodes

        
    
    def get_node(self, key):
        """
        Find the highest hash value via hash(node+key) and the node that generates the highest
        value among all nodes.
        :param key: a string key name.
        :return the highest node.
        """
        highest_node = None
        # TODO
        high_score = -1
        for node in self.nodes:
            to_be_hashed = node + key
            score = hashlib.md5(to_be_hashed).hexdigest()
            if score > high_score:
                high_score, highest_node = score, node

        return highest_node


class RendezvousHashDBClient(RendezvousHash):
    """
    This class extends from the above RendezvousHash class and
    integrates DBClient (see@client.py) with RendezvousHash so that 
    client can PUT and GET to the DB servers while the rendezvous hash shards 
    the data across multiple DB servers.
    DO NOT USE ANY STATIC CLASS VARIABLES!
    """

    def __init__(self, db_servers=None):
        """
        1. Initialize the super/parent RendezvousHash class.
        Class inheritance@http://www.python-course.eu/python3_inheritance.php
        2. Create DBClient instance for all servers and save them in a dictionary.
        :param db_servers: a list of DB servers: ['0.0.0.0:3000', '0.0.0.0:3001', '0.0.0.0:3002']
        """
        # TODO
        super(RendezvousHashDBClient,self).__init__(db_servers)
        #RendezvousHash.__init__(db_servers)
        self.dbclient_dict = {}
        for server in db_servers:
            host = server.split(':')[0]
            print server.split(':')[0]
            print server.split(':')[1]
            port = (int)(server.split(':')[1])
            client = DBClient(host=host, port=port)
            self.dbclient_dict[server] = client


    def put(self, key, value):
        """
        1. Get the highest Rendezvous node for the given key.
        2. Retrieve the DBClient instance reference by the node.
        3. Save the value into DB via client's put(). 
        :param key: a string key.
        :param value: a string key-value pair dictionary to be stored in DB. 
        :return a PutResponse - see@db.proto
        NOTE: Both key and value must be the string type.
        """
        # TODO
        node = super(RendezvousHashDBClient,self).get_node(key)
        client = self.dbclient_dict[node]
        res = client.put(key,value)
        return res

    
    def get(self, key):
        """
        1. Get the highest Rendezvous node for the given key.
        2. Retrieve the DBClient instance reference by the node.
        3. Get the value by the key via client's get(). 
        :param key: a string key.
        :param value: a string key-value pair dictionary to be stored in DB. 
        :return a GetResposne - see@db.proto
        """
        # TODO
        node = super(RendezvousHashDBClient,self).get_node(key)
        client = self.dbclient_dict[node]
        res = client.get(key)
        return res


    def info(self):
        """
        Return a list of InfoResponse from all servers.
        1. Invoke DB client's info() to retrieve server info for all servers.
        """
        server_info = []
        # TODO
        for k,v in self.dbclient_dict.items():
            client = self.dbclient_dict[k]
            server_info.append(client.info())
        return server_info

        