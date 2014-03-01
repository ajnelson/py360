
import Objects
import logging
import os

_logger = logging.getLogger(os.path.basename(__file__))

XMLNS_XTAF = "http://www.forensicsiwki.org/wiki/XTAF"
XMLNS_PY360 = "http://forensicsiwki.org/wiki/Py360"

class XTAFFileObject(Objects.FileObject):
    """A class to keep the documentation straight on XTAF extensions to DFXML FileObjects."""

    _new_properties = {
      "basename",
      "flags",
      "starting_cluster"
    }

    def __init__(self, *args, **kwargs):
        super(XTAFFileObject,self).__init__(*args, **kwargs)
        for prop in self._new_properties:
            setattr(self, prop, kwargs.get(prop))

    def to_Element(self):
        e = super(XTAFFileObject, self).to_Element()
        return e

    @property
    def basename(self):
        """The name as recorded in an on-disk directory entry."""
        return self._basename

    @basename.setter
    def basename(self, value):
        if not value is None:
            Objects._typecheck(value, str)
        self._basename = value

    @property
    def cluster_chain(self):
        return self._cluster_chain

    @cluster_chain.setter
    def cluster_chain(self, value):
        if not value is None:
            Objects._typecheck(value, list)
            for cluster in value:
                Objects._typecheck(value, int)
        self._cluster_chain = value

    @property
    def flags(self):
        """The flags as parsed from an on-disk directory entry.  Should be an int."""
        return self._flags

    @flags.setter
    def flags(self, value):
        if not value is None:
            Objects._typecheck(value, int)
        self._flags = value
        
    @property
    def starting_cluster(self):
       """Unit of measurement: Cluster."""
       return self._starting_cluster

    @starting_cluster.setter
    def starting_cluster(self, value):
        if not value is None:
            Objects._typecheck(value, int)
        self._starting_cluster = value

if __name__ == "__main__":
    tester = XTAFFileObject()
    tester.basename = "bar"
    tester.filename = "foo/bar"
