
import Objects
import logging
import os
import xml.etree.ElementTree as ET

_logger = logging.getLogger(os.path.basename(__file__))

XMLNS_XTAF = "http://www.forensicsiwki.org/wiki/XTAF"
XMLNS_PY360 = "http://forensicsiwki.org/wiki/Py360"

class XTAFFileObject(Objects.FileObject):
    """A class to keep the documentation straight on XTAF extensions to DFXML FileObjects."""

    _new_properties = {
      "basename",
      "cluster_chain",
      "flags",
      "root",
      "starting_cluster"
    }

    def __init__(self, *args, **kwargs):
        super(XTAFFileObject,self).__init__(*args, **kwargs)
        for prop in self._new_properties:
            setattr(self, prop, kwargs.get(prop))

    def to_Element(self):
        e = super(XTAFFileObject, self).to_Element()
        if not self.root is None:
            tmpel = ET.Element("xtaf:root")
            tmpel.text = str(1 if self.root else 0)
            e.append(tmpel)
        if not self.basename is None:
            tmpel = ET.Element("xtaf:basename")
            tmpel.text = self.basename
            e.append(tmpel)
        if not self.cluster_chain is None:
            tmpel = ET.Element("xtaf:cluster_chain")
            tmpel.text = ",".join(map(str, self.cluster_chain))
            e.append(tmpel)
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
                try:
                    Objects._typecheck(cluster, int)
                except:
                    _logger.debug("value = %r." % value)
                    raise
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
    def root(self):
        """Boolean.  True for root directory, False or simply null for everything else."""
        return self._root

    @root.setter
    def root(self, value):
        self._root = Objects._boolcast(value)

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
