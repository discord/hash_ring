import pytest

import random

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
    for i in xrange(500):
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


def ring_fast(num_nodes):
    r = HashRing()
    nodes = []
    for i in xrange(num_nodes):
        nodes.append(HashRingNode('test-%i' % i, num_replicas=512))

    r.add_nodes(nodes)


def ring_slow(num_nodes):
    r = HashRing()
    nodes = []
    for i in xrange(num_nodes):
        r.add_node('test-%i' % i, num_replicas=512)

    r.add_nodes(nodes)


@pytest.mark.parametrize('num_nodes', [5, 10, 25, 50, 100])
def test_ring_add_nodes_slow(benchmark, num_nodes):
    benchmark(ring_slow, num_nodes)


@pytest.mark.parametrize('num_nodes', [5, 10, 25, 50, 100])
def test_ring_add_nodes_fast(benchmark, num_nodes):
    benchmark(ring_fast, num_nodes)
