from hashlib import sha256
from urllib.parse import urlparse, urlunparse

# Based on (and should return the same results as):
# https://dev.azure.com/msresearch/Serverless-Efficiency/_git/azure-webjobs-sdk?path=/src/Microsoft.Azure.WebJobs.Host/FunctionDataCache/ConsistentHash.cs

class ConsistentHash:

    def __init__(self, members, replicas = 50):
        self._replicas = replicas
        self._create_ring(members)

    @staticmethod
    def _hash_key(key):
        bytes_key = bytes(key, 'utf-8')
        bytes_hash = sha256(bytes_key).digest()
        #Now make a signed 32-bit integer from the first 4 bytes
        #TODO: fix the order in the C# code, right now it is following
        #      the architecture
        int_key = int.from_bytes(bytes_hash[0:4],'little', signed=True)
        #print(key, "->", bytes_key.hex(), "->", bytes_hash.hex(), "->", int_key)
        return int_key

    def _create_ring(self, members):
        tmp_ring = {}
        for member in members:
            for r in range(self._replicas):
                str_key = str(member) + str(r)
                int_key = self._hash_key(str_key)
                tmp_ring[int_key] = member
        #Sort the ring and store it in a member
        self._ring = {key:val for key, val in sorted(tmp_ring.items(), key = lambda item: item[0])}

    # just recreate the ring, easier 
    def update_ring(self, members):
        self._create_ring(members)

    def get_member(self, key):
        m = self._get_member(str(key))
        print("get_member:{0} -> {1}".format(key,m))
        return m

    def print_ring_keys(self):
        print("{0} members: {1}".format(len(self._ring.keys()), self._ring.keys()))

    def print_ring(self):
        for key, val in self._ring.items():
            print("{} -> {}".format(key, val))

    # return the smallest member >= key (wrapping around if larger than last)
    # implemented as a simple binary search that returns exact and inexact matches
    def _get_member(self, key):

        ringKeys = list(self._ring.keys())

        uri_parts = list(urlparse(key))
        uri_parts[4] = ''
        uri_parts[5] = ''
        uri_base = urlunparse(uri_parts)

        low = 0
        high = len(ringKeys) - 1

        hkey = self._hash_key(uri_base)

        # literally the corner cases
        if (hkey < ringKeys[low] or hkey > ringKeys[high]):
            return self._ring[ringKeys[low]]
        
        # binary search
        while (high - low > 1):
            mid = (low + high) // 2
            if hkey == ringKeys[mid]:
                return self._ring[mid]
            if hkey > ringKeys[mid]:
                low = mid
            elif hkey < ringKeys[mid]:
                high = mid

        if (hkey == ringKeys[low]):
            return self._ring[ringKeys[low]]
        else:
            return self._ring[ringKeys[high]]

    
