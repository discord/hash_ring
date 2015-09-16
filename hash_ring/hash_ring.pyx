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

    hash_ring_t *hash_ring_create(unsigned int num_replicas, HASH_FUNCTION hash_fn)
    void hash_ring_free(hash_ring_t *ring)
    int hash_ring_add_node(hash_ring_t *ring, unsigned char *name, unsigned int name_len)
    hash_ring_node_t *hash_ring_find_node(hash_ring_t *ring, unsigned char *key, unsigned int key_len)
    int hash_ring_remove_node(hash_ring_t *ring, unsigned char *name, unsigned int name_len)

HASH_FUNCTION_SHA1 = _HASH_FUNCTION_SHA1
HASH_FUNCTION_MD5 = _HASH_FUNCTION_MD5

cdef class HashRing:
    cdef hash_ring_t *_ring

    def __cinit__(self, num_replicas=128, hash_fn=HASH_FUNCTION_MD5):
        self._ring = hash_ring_create(num_replicas, hash_fn)

    def __dealloc__(self):
        hash_ring_free(self._ring)

    def add_node(self, name):
        return hash_ring_add_node(self._ring, name, len(name)) == HASH_RING_OK

    def remove_node(self, name):
        return hash_ring_remove_node(self._ring, name, len(name)) == HASH_RING_OK

    def find_node(self, key):
        if not isinstance(key, bytes):
            key = bytes(key)
        node = hash_ring_find_node(self._ring, key, len(key))
        if node:
            return node.name[:node.name_len]
