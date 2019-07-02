#!/usr/bin/env python3.7

import os
import attr
from sqlalchemy import create_engine
from typing import Any
from functools import partial
from pathlib import Path


engine = create_engine('sqlite:///:memory:', echo=True)


def pathify(path):
    return Path(os.fsdecode(path))


@attr.s(auto_attribs=True)
class Dnode():
    inode:    int = attr.ib(converter=int)                                                                    # noqa: E241
    full:   float = attr.ib(converter=float)                                                                  # noqa: E241
    type:     str = attr.ib(converter=partial(str, encoding='utf8'))                                          # noqa: E241
    flags:    str = attr.ib(converter=attr.converters.optional(partial(str, encoding='utf8')), default=None)  # noqa: E241
    maxblkid: int = attr.ib(converter=attr.converters.optional(int), default=None)                            # noqa: E241
    path:   bytes = attr.ib(converter=attr.converters.optional(pathify), default=None)                        # noqa: E241

    def __attrs_post_init__(self):
        self._initialized = True

    def __setattr__(self, name: str, value: Any) -> None:
        """Call the converter and validator when we set the field (by default it only runs on __init__)"""
        if not hasattr(self, "_initialized"):
            super().__setattr__(name, value)
            return
        for attribute in [a for a in getattr(self.__class__, '__attrs_attrs__', []) if a.name == name]:
            attribute_type = getattr(attribute, 'type', None)
            if attribute_type is not None:

                attribute_converter = getattr(attribute, 'converter', None)
                if attribute_converter is not None:
                    value = attribute_converter(value)

            attribute_validator = getattr(attribute, 'validator', None)
            if attribute_validator is not None:
                attribute_validator(self, attribute, value)

        super().__setattr__(name, value)


# parsing zdb output, which is arb bytes (except NULL) because it contains filesystem paths
def test():
    # state machine to parse out props loops over each zdb ouput line,
    # adding stuff to the dn until the next dn (inode) comes along
    dn = Dnode(b'1', b"100.0", b"ZFS plain file")  # these values are required, the rest may or may not exist for a inode
    dn.flags = b'USED_BYTES'
    # leaving dn.maxblkid None
    dn.path = b'/var/log/messages'                 # could be a single bytes([254]), all valid file names must be accepted

    print("dn:", dn)

    # commit() dn to sqlite table here via sqlalchemy, either using the orm or core
    # looking for a way to orm-ify Dnode while keeping all the attr stuff intact


if __name__ == '__main__':
    test()

