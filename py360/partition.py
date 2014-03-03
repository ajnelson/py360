#!/usr/bin/python

""" 
    This module has the classes associated with parsing XTAF parititions
    To use this class try something like:
    from partition import *
    xtafpart = Partition('/mnt/data/201010.bin') 
"""

import sys
import mmap
import struct
from threading import Lock
from cStringIO import StringIO
import Objects
import XTAFDFXML
import hashlib
import copy
import logging
import os
_logger = logging.getLogger(os.path.basename(__file__))

# TODO: Optional thread safety
class XTAFFD(object):
    """ A File-like object for representing FileObjs """
    def __init__(self, partition, fileobj):
        self.pointer = 0
        self.fileobj = fileobj
        self.partition = partition

    def read(self, length=-1):
        buf = self.partition.read_file(fileobj = self.fileobj, size=length, offset=self.pointer)
        self.pointer += len(buf)
        return buf

    def seek(self, offset, whence=0):
        if whence == 0:
            self.pointer = offset
        if whence == 1:
            self.pointer = self.pointer + offset
        if whence == 2:
            self.pointer = self.fileobj.fr.fsize - offset

        if self.pointer > self.fileobj.fr.fsize:
            self.pointer = self.fileobj.fr.fsize
        if self.pointer < 0:
            self.pointer = 0

    def tell(self):
        return self.pointer

class FileRecord(object):
    """FileRecord is straight off of the disk (but with everything in host byte order)"""
    def __str__(self):
        return "XTAF FileRecord: %s" % self.filename

    def __init__(self, **kwargs):
        self.fnsize = kwargs.get("fnsize")
        self.attribute = kwargs.get("attribute")
        self.filename = kwargs.get("filename")
        self.cluster = kwargs.get("cluster")
        self.fsize = kwargs.get("fsize")
        self.mtime = kwargs.get("mtime")
        self.mdate = kwargs.get("mdate")
        self.ctime = kwargs.get("ctime")
        self.cdate = kwargs.get("cdate")
        self.atime = kwargs.get("atime")
        self.adate = kwargs.get("adate")
        self.fileobject = None #Set in parse_file_records, but left null here as a placeholder.

    def isDirectory(self):
        if self.fsize == 0:
            return True
        return False

class FileObj(object):
    """ FileObj is a container with a FileRecord and a list of clusters """
    def __str__(self):
        return "XTAF File: %s" % self.fr

    def __init__(self, fr, clusters):
        self.fr = fr
        self.clusters = clusters

    def isDirectory(self):
        return False

class Directory(FileObj):
    """ Directory is a FileObj with a dict of FileObj """
    def __str__(self):
        return "%s (Directory)" % (super(Directory, self).__str__())

    def __init__(self, fr, clusters):
        super(Directory, self).__init__(fr, clusters)
        self.files = {}
        self.root = False

    def isDirectory(self):
        return True

class Partition(object):
    """
        Main class representing the partition
        The allfiles member has a dictionary of all the files in the partition
        The rootfile member contains a directory object that represents the root directory 
    """
    def __str__(self):
        return "XTAF Partition: %s" % self.filename

    def __init__(self, filename, threadsafe=False, precache=False):
        self.filename = filename
        self.threadsafe = threadsafe
        self.SIZE_OF_FAT_ENTRIES = 4
        self.volume_object = XTAFDFXML.XTAFVolumeObject()

        #TODO: Error checking
        fd = open(filename, 'r') # The 'r' is very imporant
        if fd.read(4) != 'XTAF':
            start = 0x130EB0000L # TODO: Improve this detection mechanism
            self.volume_object.partition_offset = start
        else:
            start = 0
            #self.volume_object.partition_offset intentionally left null.

        #Parse superblock
        fd.seek(start, 0)
        if fd.read(4) != "XTAF":
            raise ValueError("Partition not found at offset %r." % start)
        raw_vol_id = fd.read(4)
        raw_sectors_per_cluster = fd.read(4)
        raw_num_fats = fd.read(4)
        self.volume_object.volume_id = struct.unpack(">I", raw_vol_id)[0]
        self.sectors_per_cluster = struct.unpack(">I", raw_sectors_per_cluster)[0]
        num_fats = struct.unpack(">I", raw_num_fats)[0]
        if num_fats > 1:
            _logger.error("Encountered an XTAF partition with more than one FAT (%r).  Have not encountered this before.  Expect strange results!" % num_fats)

        #Determine FAT dimensions and rootdir location
        fat = start + 0x1000L
        fd.seek(0, 2)
        end = fd.tell()
        rootdir = -(-((end - start) >> 12L) & -0x1000L) + fat #TODO: Understand this better
        size = end - rootdir
        fatsize = size >> 14L

        # This doesn't work because unlike the C version of mmap you can't give it a 64 bit offset
        #fatfd = mmap.mmap(fd.fileno(), fatsize, mmap.PROT_READ, mmap.PROT_READ, offset=fat)
        # So we have to keep the whole FAT in memory during processing
        fd.seek(fat, 0)
        fatdata = fd.read(fatsize * self.SIZE_OF_FAT_ENTRIES)
        fd.seek(0, 0)

        # Setup internal variables
        self.root_dir_cluster = 1
        self.start = start
        self.fat = fat
        self.root_dir = rootdir
        self.size = size
        self.fat_num = fatsize
        self.fd = fd
        self.fat_data = fatdata # <- FAT is in BIG ENDIAN
        self.allfiles = {}
        self.lock = Lock()
        #self.rootfile = self.parse_directory()
        self.rootfile = self.init_root_directory(recurse = precache)

        #Create a virtual file for the FAT
        fatfileobject = XTAFDFXML.XTAFFileObject()
        fatfileobject.filename = "$FAT1"
        fatfileobject.filesize = int(fatsize * self.SIZE_OF_FAT_ENTRIES)
        fatfileobject.name_type = "v"
        fatfileobject.data_brs = Objects.ByteRuns()
        fbr = Objects.ByteRun()
        fbr.len = fatfileobject.filesize
        fbr.fs_offset = int(fat - start)
        if not self.volume_object.partition_offset is None:
            fbr.img_offset = int(fbr.fs_offset + self.volume_object.partition_offset)
        fatfileobject.data_brs.append(fbr)
        self.volume_object.append(fatfileobject)

    def cluster_to_disk_offset(self, cluster, partition_offset):
        return (cluster - 1 << 14L) + self.root_dir + partition_offset

    def read_cluster(self, cluster, length=0x4000, offset=0L):
        """ Given a cluster number returns that cluster """
        error_msg = None
        if length + offset <= 0x4000: #Sanity check
            diskoffset = self.cluster_to_disk_offset(cluster, offset)
            # Thread safety is optional because the extra function calls are a large burden
            if self.threadsafe:
                self.lock.acquire() 

            try:
                self.fd.seek(diskoffset)
                buf = self.fd.read(length)
            except IOError as e:
                buf = ""
                error_msg = "I/O error (%d): %s" % (e.errno, e.strerror)

            if self.threadsafe:
                self.lock.release()

            if not error_msg is None:
                _logger.error(error_msg)
            return buf
        else:
            return ""

    #TODO: Refactor into something smaller
    def read_file(self, filename=None, fileobj=None, size=-1, offset=0):
        """ Reads an entire file given a filename or fileobj """
        #TODO: Error checking
        if not fileobj: 
            fileobj = self.get_file(filename)

        if size == -1:
            if fileobj.isDirectory():
                size = 512 * self.sectors_per_cluster # Read the whole directory (all the clusters)
            else:
                size = fileobj.fr.fsize # Read the whole file (skip the slack space)

        if len(fileobj.clusters) == 0: # Initialise cluster list if necessary
            fileobj.clusters = self.get_clusters(fileobj.fr)
            if len(fileobj.clusters) == 0: # Check the return of get_clusters
                print "Reading Empty File"
                return ""

        clusters_to_skip = offset // 0x4000
        offset %= 0x4000
        buf = StringIO() 
        try:
            readlen = min(0x4000, size)
            buf.write(self.read_cluster(fileobj.clusters[clusters_to_skip], readlen, offset))
            size -= readlen
            for cl in fileobj.clusters[clusters_to_skip+1:]:
                if size <= 0:
                    break # If we're finished, stop reading clusters
                readlen = min(0x4000, size)
                buf.write(self.read_cluster(cl, readlen, 0))
                size -= readlen
            return buf.getvalue()
        except IndexError:
            print "Read overflow?", len(fileobj.clusters), clusters_to_skip
            return buf.getvalue()

    def get_clusters(self, fr):
        """ Builds a list of the clusters a file hash by parsing the FAT """
        if fr.cluster == 0:
            print "Empty file"
            return []
        clusters = [fr.cluster]
        cl = 0x0
        cl = fr.cluster
        cldata = ''
        while cl & 0xFFFFFFF != 0xFFFFFFF:
            cl_off = cl * self.SIZE_OF_FAT_ENTRIES 
            cldata = self.fat_data[cl_off:cl_off + self.SIZE_OF_FAT_ENTRIES]
            if len(cldata) == 4:
                cl = struct.unpack(">I", cldata)[0] 
                if cl & 0xFFFFFFF != 0xFFFFFFF:
                    clusters.append(cl)
            else:
                if fr.filename[0] != '~':
                    print "get_clusters fat offset warning %s %x vs %x, %x" %\
                          (fr.filename, cl_off, len(self.fat_data), len(cldata))
                cl = 0xFFFFFFF
        return clusters

    def open_fd(self, filename):
        f = self.get_file(filename)
        """ Return an XTAFFD object for a file """
        if f != None:
            return XTAFFD(self, f)
        else:
            return None

    def parse_file_records(self, data, parent_file_object=None):
        """
            parent_file_object: Expected type is XTAFFileObject.
            While not end of file records
            Create a file record object
            Return list of file records
            Date format: 
        """
        file_records = []
        pos = 0
        while pos + 64 < len(data): # FileRecord struct offsets
            fnlen = data[pos]
            flags = data[pos+1]
            recorded_name = data[pos+2:pos+2+42].strip("\xff\x00")
            alloc = None
            if ord(fnlen) == 0xE5: # Handle deleted files
                alloc = False
                name = '~' + recorded_name
            elif ord(fnlen) > 42: # Technically >42 should be an error condition
                _logger.debug("Encountered a directory entry with fnlen >42 (%r), at position %r.  Ceasing parsing this directory." % (fnlen, pos))
                break
            elif ord(fnlen) == 0: # A vacant entry, maybe the end of the directory?
                pos += 64
                continue
            else: 
                alloc = True
                name = recorded_name # Ignoring fnlen is a bit wasteful
            cl = struct.unpack(">I", data[pos+0x2c:pos+0x2c+4])[0]
            size = struct.unpack(">I", data[pos+0x30:pos+0x30+4])[0]
            creation_date = struct.unpack(">H", data[pos+0x34:pos+0x34+2])[0]
            creation_time = struct.unpack(">H", data[pos+0x36:pos+0x36+2])[0]
            access_date = struct.unpack(">H", data[pos+0x38:pos+0x38+2])[0]
            access_time = struct.unpack(">H", data[pos+0x3A:pos+0x3A+2])[0]
            update_date = struct.unpack(">H", data[pos+0x3C:pos+0x3C+2])[0]
            update_time = struct.unpack(">H", data[pos+0x3E:pos+0x3E+2])[0]

            #if not (fnlen == '\xff' and flags == '\xff') and not fnlen == '\x00':
            if (ord(fnlen) < 43 and ord(fnlen) != 0) or (ord(fnlen) == 0xE5):
                fr = FileRecord(fnsize=fnlen, attribute=flags, filename=name, cluster=cl,\
                                fsize=size, mtime=update_time, mdate=update_date,\
                                adate=access_date, atime=access_time,\
                                cdate=creation_date, ctime=creation_time)
                file_records.append(fr)

                #Populate FileObject here
                import xboxtime
                fobj = XTAFDFXML.XTAFFileObject()
                if parent_file_object:
                    fobj.parent_object = parent_file_object

                #DFXML base fields straight from directory entry parse
                fobj.mtime = xboxtime.fatx2iso8601time(update_time, update_date)
                fobj.atime = xboxtime.fatx2iso8601time(access_time, access_date)
                fobj.crtime = xboxtime.fatx2iso8601time(creation_time, creation_date)
                fobj.filesize = size
                fobj.alloc_name = alloc
                fobj.alloc_inode = alloc
                fobj.volume_object = self.volume_object

                #XTAF extension fields straight from directory entry parse
                fobj.starting_cluster = cl
                fobj.basename = recorded_name
                fobj.flags = struct.unpack(">b", data[pos+1])[0]

                #Fields that require some computation

                fobj.name_type = "d" if (fobj.flags & 16) else "r"

                md5obj = hashlib.md5()
                sha1obj = hashlib.sha1()
                fobj.cluster_chain = self.get_clusters(fr)
                fobj.data_brs = Objects.ByteRuns()
                fobj.data_brs.facet = "data"
                aborted_cluster_walk = None
                file_offset = 0

                #Determine number of bytes that should be read and called file data
                #For regular files, this is simply what's recorded in the directory entry.
                #For directories, we'll call it the size of a cluster * the length of the cluster chain.
                whole_cluster_length = 512 * self.sectors_per_cluster
                bytes_to_read = None
                if fobj.name_type == "r":
                    bytes_to_read = fobj.filesize
                elif fobj.name_type == "d":
                    bytes_to_read = whole_cluster_length * len(fobj.cluster_chain)
                else:
                    raise ValueError("No rule available for defining file length in bytes based on name type %r." % fobj.name_type)
                for cluster in fobj.cluster_chain:
                    if cluster == 0:
                        aborted_cluster_walk = True
                        msg = "Cluster chain includes cluster 0.  Skipping this entire cluster chain; no hash will be recorded."
                        fobj.error = msg
                        _logger.warning(msg + "  (Recorded in XML.)")
                        break

                    bytes_to_read_from_cluster = min(whole_cluster_length, bytes_to_read)
                    bytes_to_read -= bytes_to_read_from_cluster

                    #Note that read_cluster was designed to return empty strings on I/O errors.
                    cluster_data = self.read_cluster(cluster, whole_cluster_length)[:bytes_to_read_from_cluster]
                    md5obj.update(cluster_data)
                    sha1obj.update(cluster_data)
                    br = Objects.ByteRun()
                    br.len = bytes_to_read_from_cluster
                    br.file_offset = file_offset
                    fs_offset = int(self.cluster_to_disk_offset(cluster, fobj.volume_object.partition_offset or 0))
                    #_logger.debug("fs_offset = %r." % fs_offset)
                    br.fs_offset = fs_offset
                    if not fobj.volume_object.partition_offset is None:
                        br.img_offset = br.fs_offset + fobj.volume_object.partition_offset
                    fobj.data_brs.glom(br)
                    file_offset += bytes_to_read_from_cluster
                if not aborted_cluster_walk:
                    fobj.md5 = md5obj.hexdigest()
                    fobj.sha1 = sha1obj.hexdigest()
                    if bytes_to_read > 0:
                        msg = "After walking the cluster chain, there are %d bytes remaining to read.  The hashes are likely incorrect." % bytes_to_read
                        fobj.error = msg
                        _logger.warning(msg + "  (Recorded in XML.)")

                #Compute full path
                full_path_parts = [fobj.basename]
                if fobj.parent_object:
                    parent_pointer = fobj.parent_object
                    while not parent_pointer is None:
                        if parent_pointer.basename is None:
                            break
                        full_path_parts.insert(0, parent_pointer.basename or "")
                        parent_pointer = parent_pointer.parent_object
                fobj.filename = "/".join(full_path_parts)

                #Define name and metadata byte runs
                if fobj.parent_object:
                    containing_byte_run = None
                    for br in fobj.parent_object.data_brs:
                        if br.file_offset + br.len > pos:
                            containing_byte_run = br
                            break
                    name_br = Objects.ByteRun()
                    name_br.len = 64
                    name_br.fs_offset = containing_byte_run.fs_offset + pos - containing_byte_run.file_offset
                    if not fobj.volume_object.partition_offset is None:
                        name_br.img_offset = name_br.fs_offset + fobj.volume_object.partition_offset
                    fobj.name_brs = Objects.ByteRuns()
                    fobj.name_brs.facet = "name"
                    fobj.name_brs.append(name_br)
                    #In XTAF, name and metadata byte runs are the same.
                    fobj.meta_brs = copy.deepcopy(fobj.name_brs)
                    fobj.meta_brs.facet = "meta"
                #TODO
                #fobj.inode = 

                #Record
                self.volume_object.append(fobj)
                fr.fileobject = fobj
            else:
                pass

            pos += 64

        return file_records


    def walk(self, path = '/'):
        """ A generator that will return every fileobj on a system below path.
            This is designed to be used instead of iterating over self.allfiles. 
            self.allfiles can still be used if the partition is created with precache = True
            Using this will eliminate much of the advantage of precache = False.
            The only remaining speedup will be the lazy caching of file cluster lists
        """
        f = self.get_file(path)
        if f == None or not f.isDirectory():
            return
        files = [f]

        while len(files) > 0:
            f = files.pop(0)
            if f.isDirectory():
                if not f.root and len(f.clusters) == 0:
                    f = self.parse_directory(f) 
                files = files + f.files.values()
            yield f.fullpath

        return 

            


    def get_file(self, filename):
        """ Returns a fileobj from a filename. 
            Checks allfiles and if it isn't present starts walking the allfiles directory.
            Not the same as self.allfiles[filename] anymore. """
        if filename in self.allfiles: 
            currentfile = self.allfiles[filename]
            if currentfile.isDirectory() and not currentfile.root and len(currentfile.clusters) == 0:
                # If we're asked for a directory, initialise it before returning
                currentfile = self.parse_directory(currentfile) 
            return currentfile # A previously accessed file
        else:
            return self.walk_for_file(filename)

    def walk_for_file(self, filename):
        """ Walks the file system parsing directories where necessary looking for a fileobj """
        # Parse subdirectories looking for the requested file
        file_components = filename[1:].split("/") # Skip first slash
        currentfile = self.rootfile
        for component in file_components:
            #print "f:%s\t c:%s\t" % (filename, component),  currentfile, self.rootfile
            if currentfile == None:
                break
            # If this is a directory (that isn't root) and it has no clusters listed, try to initialise it
            if currentfile.isDirectory() and not currentfile.root and len(currentfile.clusters) == 0:
                currentfile = self.parse_directory(currentfile)
            try:
                currentfile = currentfile.files[component]
            except KeyError:
                currentfile = None

        if currentfile != None and currentfile.isDirectory():
            print "Initialising: %s" % filename
            currentfile = self.parse_directory(currentfile) # If we're asked for a directory, initialise it before returning

        return currentfile


    def init_root_directory(self, recurse = False):
        """ Creates the root directory object and calls parse_directory on it """
        directory = Directory(None, [self.root_dir_cluster])
        directory.root = True
        directory.fullpath = '/'
        self.allfiles[directory.fullpath] = directory

        #Create a DFXML object for the root directory (called before parse_directory so it appears first in the file object stream)
        fobj = XTAFDFXML.XTAFFileObject()
        fobj.root = True
        fobj.name_type = "d"
        fobj.volume_object = self.volume_object
        fobj.alloc_inode = True
        fobj.alloc_name = True
        fobj.cluster_chain = [self.root_dir_cluster]
        fobj.data_brs = Objects.ByteRuns()
        br = Objects.ByteRun()
        br.len = 512 * self.sectors_per_cluster
        br.file_offset = 0
        br.fs_offset = int(self.cluster_to_disk_offset(self.root_dir_cluster, fobj.volume_object.partition_offset or 0))
        if fobj.volume_object.partition_offset:
            br.img_offset = br.fs_offset + fobj.volume_object.partition_offset
        fobj.data_brs.glom(br)
        fobj.data_brs.facet = "data"
        self.volume_object.append(fobj)

        directory.fr = FileRecord() #A mostly-null object, since the root has no directory entry.
        directory.fr.fileobject = fobj
        directory = self.parse_directory(directory, recurse = recurse)
        return directory 

    #TODO: Refactor this to something smaller
    def parse_directory(self, directory = None, recurse = False):
        """ Parses a single directory, optionally it can recurse into subdirectories.
            It populates the allfile dict and parses the directories and file records of the directory """
        dirs_to_process = []
        if directory == None:
            return None
        else:
            dirs_to_process.append(directory)

        # For each directory to process (will be only one unless recurse is True)
        while len(dirs_to_process) > 0:
            d = dirs_to_process.pop(0)
            if d.root:
                directory_data = self.read_cluster(self.root_dir_cluster)
            else:
                directory_data = self.read_file(fileobj = d)

            # Parse the file records returned and optionally requeue subdirectories
            parent_to_pass = None
            if d.fr and d.fr.fileobject:
                parent_to_pass = d.fr.fileobject
                _logger.debug("Passing a parent object reference.")
            file_records = self.parse_file_records(directory_data, parent_to_pass)
            for fr in file_records:
                if fr.isDirectory():
                    d.files[fr.filename] = Directory(fr, [])
                    if recurse:
                        dirs_to_process.append(d.files[fr.filename])
                else:
                    d.files[fr.filename] = FileObj(fr, [])
                if d.root:
                    d.files[fr.filename].fullpath = d.fullpath + fr.filename
                else:
                    d.files[fr.filename].fullpath = d.fullpath + '/' + fr.filename
                self.allfiles[d.files[fr.filename].fullpath] = d.files[fr.filename]
        return directory

