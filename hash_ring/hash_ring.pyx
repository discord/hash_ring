from libc.stdlib cimport malloc, free

cdef extern from 'hash_ring.h':
    cdef int HASH_RING_OK
    cdef int HASH_RING_ERR

    ctypedef unsigned char HASH_FUNCTION

    cdef HASH_FUNCTION _HASH_FUNCTION_SHA1 'HASH_FUNCTION_SHA1'
    cdef HASH_FUNCTION _HASH_FUNCTION_MD5 'HASH_FUNCTION_MD5'

    ctypedef struct hash_ring_t:
        pass

    ctypedef struct hash_ring_node_t:
        unsigned char *name
        unsigned int name_len 'nameLen'
        unsigned int num_replicas 'numReplicas'

    hash_ring_t *hash_ring_create(HASH_FUNCTION hash_fn)
    void hash_ring_free(hash_ring_t *ring)
    int hash_ring_add_node(hash_ring_t *ring, unsigned char *name, unsigned int name_len, unsigned int num_replicas)
    hash_ring_node_t *hash_ring_find_node(hash_ring_t *ring, unsigned char *key, unsigned int key_len)
    int hash_ring_find_nodes(hash_ring_t *ring, unsigned char *key, unsigned int key_len, hash_ring_node_t *nodes[], unsigned int num)
    int hash_ring_remove_node(hash_ring_t *ring, unsigned char *name, unsigned int name_len)
    void hash_ring_print(hash_ring_t *ring)

HASH_FUNCTION_SHA1 = _HASH_FUNCTION_SHA1
HASH_FUNCTION_MD5 = _HASH_FUNCTION_MD5

cdef class HashRing:
    cdef hash_ring_t *_ring
    cdef unsigned int _default_num_replicas

    def __cinit__(self, default_num_replicas=128, hash_fn=HASH_FUNCTION_MD5):
        self._ring = hash_ring_create(hash_fn)
        self._default_num_replicas = default_num_replicas

    def __dealloc__(self):
        hash_ring_free(self._ring)

    def add_node(self, name, num_replicas=None):
        if num_replicas is None:
            num_replicas = self._default_num_replicas

        return hash_ring_add_node(self._ring, name, len(name), num_replicas) == HASH_RING_OK

    def remove_node(self, name):
        return hash_ring_remove_node(self._ring, name, len(name)) == HASH_RING_OK

    def print_ring(self):
        hash_ring_print(self._ring)

    def find_node(self, key):
        if not isinstance(key, bytes):
            key = bytes(key)
        node = hash_ring_find_node(self._ring, key, len(key))
        if node:
            return node.name[:node.name_len]

    def find_nodes(self, key, num=1):
        if not isinstance(key, bytes):
            key = bytes(key)
        cdef hash_ring_node_t **nodes = <hash_ring_node_t **>malloc(sizeof(hash_ring_node_t *) * num)
        if not nodes:
            raise MemoryError()
        try:
            n = hash_ring_find_nodes(self._ring, key, len(key), nodes, num)
            return [node.name[:node.name_len] for node in nodes[:n]] if n > -1 else []
        finally:
            free(nodes)