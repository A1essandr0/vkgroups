from invoke import Collection

from . import local, test

ns = Collection()
ns.add_collection(local)
ns.add_collection(test)
