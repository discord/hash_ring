import pytest

import random
from collections import Counter

from hash_ring import HashRing, HashRingNode


def test_hash_ring_basic_flow():
    ring = HashRing()
    # A ring with no node can't find any nodes.
    assert ring.find_node(1) is None

    # Adding a node should find the node.
    assert ring.add_node('test')
    assert ring.find_node(1) == 'test'

    # Removing it should work.
    assert ring.remove_node('test')
    assert ring.find_node(1) is None


def test_hash_ring_add():
    ring = HashRing()

    assert ring.get_num_items() == 0
    assert ring.get_node('test') is None
    assert ring.add_node('test', num_replicas=128)
    node = ring.get_node('test')
    assert node.name == 'test'
    assert node.num_replicas == 128

    assert ring.get_num_items() == 128
    assert ring.remove_node('test')
    assert ring.get_num_items() == 0

    assert ring.add_node('test', num_replicas=64)
    node = ring.get_node('test')
    assert node.name == 'test'
    assert node.num_replicas == 64
    assert ring.get_num_items() == 64


def test_hash_ring_stress_test():
    replica_sizes = [2 ** i for i in xrange(10)]

    ring = HashRing()
    config = []
    for i in xrange(20):
        name = 'test-%i' % i
        num_replicas = replica_sizes[hash(name) % len(replica_sizes)]
        config.append((name, num_replicas))

    total_items = 0
    for name, num_replicas in config:
        total_items += num_replicas
        assert ring.add_node(name, num_replicas=num_replicas)
        assert ring.get_num_items() == total_items

    random.shuffle(config)
    for name, num_replicas in config:
        assert ring.remove_node(name)
        total_items -= num_replicas
        assert ring.get_num_items() == total_items

    assert total_items == 0


def test_hash_ring_stress_fast():
    replica_sizes = [2 ** i for i in xrange(10)]

    ring = HashRing()
    nodes = []
    total_items = 0
    for i in xrange(20):
        name = 'test-%i' % i
        num_replicas = replica_sizes[hash(name) % len(replica_sizes)]
        nodes.append(HashRingNode(name=name, num_replicas=num_replicas))
        total_items += num_replicas

    ring.add_nodes(nodes)
    assert ring.get_num_items() == total_items
    random.shuffle(nodes)
    for node in nodes:
        assert ring.remove_node(node.name)
        total_items -= node.num_replicas
        assert ring.get_num_items() == total_items

    assert total_items == 0


def gen_hash_ring_fast(nodes):
    ring = HashRing()
    ring.add_nodes([n[0] for n in nodes])
    return ring


def gen_hash_ring_slow(nodes):
    ring = HashRing()
    for node, _ in nodes:
        ring.add_node(node.name, node.num_replicas)
    return ring


N = lambda name, c, num_replicas=512: (HashRingNode(name=name, num_replicas=num_replicas), c)


@pytest.mark.parametrize('ring_generator', [gen_hash_ring_fast, gen_hash_ring_slow])
@pytest.mark.parametrize('nodes', [
    (N('test-1', 32992), N('test-2', 33680), N('test-3', 33328)),
    (N('test-1', 10557, num_replicas=128), N('test-2', 44721), N('test-3', 44722)),
    (N('test-1', 49910, num_replicas=1024), N('test-2', 25683), N('test-3', 24407)),
    # These test wildly imbalanced ring configurations.
    (N('test-1', 21026, num_replicas=1), N('test-2', 48816, num_replicas=1), N('test-3', 30158, num_replicas=1)),
    (N('test-1', 99635, num_replicas=1024), N('test-2', 271, num_replicas=1), N('test-3', 94, num_replicas=1)),
])
def test_hash_ring_distributions(ring_generator, nodes):
    ring = ring_generator(nodes)

    c = Counter()
    for i in xrange(100000):
        node_name = ring.find_node(i)
        c[node_name] += 1

    for node, count in nodes:
        assert c[node.name] == count, 'Expected %i nodes for %s, got %s' % (count, node.name, c[node.name])
