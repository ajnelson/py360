
import sys
import logging
import os
from py360 import partition
from py360 import Objects
from py360 import XTAFDFXML

_logger = logging.getLogger(os.path.basename(__file__))

def main():
    p = partition.Partition(args.infile)
    d = Objects.DFXMLObject()
    d.add_namespace("xtaf", XTAFDFXML.XMLNS_XTAF)
    d.add_namespace("py360", XTAFDFXML.XMLNS_PY360)
    d.volumes.append(p.volume_object)

    need_cleanup = None
    if args.outfile is None:
        xml_fh = sys.stdout
    else:
        need_cleanup = True
        xml_fh = open(args.outfile, "w")

    d.print_dfxml(xml_fh)

    if need_cleanup:
        xml_fh.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument("infile")
    parser.add_argument("outfile", nargs="?")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

    main()
