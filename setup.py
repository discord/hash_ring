# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from setuptools.extension import Extension

src = [
    'contrib/md5.c',
    'contrib/sha1.c',
    'contrib/sort.c',
    'contrib/hash_ring.c',
]

try:
    from Cython.Distutils import build_ext
    cmdclass = {'build_ext': build_ext}
    src.append('hash_ring/hash_ring.pyx')
except ImportError:
    build_ext = None
    cmdclass = {}
    src.append('hash_ring/hash_ring.c')

ext = Extension('hash_ring.hash_ring', src, include_dirs=['contrib'])

setup(
    name='hash_ring',
    version='0.0.4',
    packages=find_packages(),
    zip_safe=False,
    cmdclass=cmdclass,
    ext_modules=[ext],
    install_requires=["six~=1.15"],
    #tests_require=['pytest'],
    #setup_requires=['pytest-runner', 'pytest-benchmark']
)
