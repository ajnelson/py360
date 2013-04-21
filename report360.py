"""
A class to output information about py360 types
Report360.document_image processes an entire disk image
This is a quick conversion from the previous solution of a few functions.
This version allows function reuse and swaps prints with a call to Report360.output to allow preprocessing
"""

__version__ = "0.1.0"

import time, os, sys
from py360 import xdbf, partition, account, stfs, xboxmagic, xboxtime
from cStringIO import StringIO

import hashlib

FILE_ID_COUNTER = 0

class Report360:
    """ A class to output information about py360 types """
    def __init__(self, filename = None, image_directory = None, out = sys.stdout, err = sys.stderr):
        self.filename = filename
        self.image_directory = image_directory
        self.outfd = out
        self.errfd = err
        self.xmlfd = None
    
    def init_dfxml(self, use_xml):
        if use_xml:
            self.xmlfd = open("py360out.dfxml", "w")

    def output(self, string, fd = None):

        if fd == None:
            fd = self.outfd

        if type(string) != type(""):
            string = str(string)

        # This is my ugly hack for ensuring output is in UTF-8. The underlying unicode problem remains.
        fd.write("%s\n" % "".join([x for x in string if (ord(x)>=32 and ord(x)<127) or x == '\t' or x == '\n']))

    def print_account(self, acc):
        """ Prints out information from an Account block """
        
        self.output("\n*********************")
        self.output(acc)
        self.output("*********************")
        self.output("XUID: %s" % acc.xuid)
        if acc.live_account:
            self.output("Account Type: %s" % acc.live_type)
        else:
            self.output("Account Type: Local")
        self.output("Console Type: %s" % acc.console_type)
        if acc.passcode:
            self.output("Passcode: %s" % acc.passcode)
        
    def xprint(self, outstring):
        """ Prints out DFXML if the XML file descriptor is live """
        if self.xmlfd:
            self.xmlfd.write(outstring)
            self.xmlfd.write("\n")

    def print_xtaf(self, part):
        """ Prints out information about an XTAF partition """
        global FILE_ID_COUNTER

        self.output("\n*********************")
        self.xprint("""  <volume offset="%s">""" % str(part.start))
        self.xprint("    <partition_offset>%s</partition_offset>" % str(part.start))
        self.xprint("    <xtaf:root_directory_offset>%u</xtaf:root_directory_offset><!--This is %d 512-byte sectors from the start of the file system.-->" % (part.rel_root_dir, part.rel_root_dir/512))
#        self.xprint("    <block_size></block_size>") #TODO report cluster size with the sectors-per-cluster value
#TODO
#      <ftype>256</ftype>
        self.xprint("    <ftype_str>XTAF</ftype_str>") #TODO Clarify XTAF16 or XTAF32
#      <block_count>235516</block_count>
#      <first_block>0</first_block>
#      <last_block>235515</last_block>

        self.output(part)
        self.output("*********************")
        self.output("\nFILE LISTING")
        #for filename in part.allfiles:
        for filename in part.walk():
            fi = part.get_file(filename)
            self.xprint("    <fileobject>")
#TODO
#self.xprint("      <parent_object>")
#self.xprint("        <inode>2</inode>")
#self.xprint("      </parent_object>")
            #Build filename for XML; py360's built-in names use a leading-tilde convention that other tools don't
            if fi.fr:
                dfxml_fn_parts = [fi.fr.xmlname]
                fr_pointer = fi.fr
                while fr_pointer.parent is not None:
                    dfxml_fn_parts.append(fr_pointer.parent.xmlname)
                    fr_pointer = fr_pointer.parent
                dfxml_fn_parts.reverse()
                dfxml_fn = "/".join(dfxml_fn_parts)
                self.xprint("      <filename>%s</filename>" % dfxml_fn) #DFXML filenames omit the leading "/"
                self.xprint("      <xtaf:flags>%r</xtaf:flags>" % fi.fr.attribute)
            self.xprint("      <py360:filename>%s</py360:filename>" % filename[1:]) #DFXML filenames omit the leading "/"
#      <partition>1</partition>
            self.xprint("      <id>%d</id>" % FILE_ID_COUNTER)
            FILE_ID_COUNTER += 1
            name_type = "-" #TODO This appears in some Fiwalk output for unallocated files
            if fi.isDirectory():
                name_type = "d"
            else:
                name_type = "r"
            self.xprint("      <name_type>%s</name_type>" % name_type)
            if fi.fr:
                self.xprint("      <filesize>%d</filesize>" % fi.fr.fsize)
                if fi.fr.allocated:
                    self.xprint("      <alloc>1</alloc>")
                else:
                    self.xprint("      <alloc>0</alloc>")
#TODO
#self.xprint("      <used>1</used>")
#self.xprint("      <inode>2</inode>")
#self.xprint("      <meta_type>2</meta_type>")
#self.xprint("      <nlink>18</nlink>")
#                self.xprint("      <uid>0</uid>") #XTAF doesn't really have a user id
#                self.xprint("      <gid>0</gid>") #XTAF doesn't really have a group id
                self.xprint("      <mtime prec=\"2\">%s</mtime>" % xboxtime.fatx2iso8601time(fi.fr.mtime, fi.fr.mdate))
                self.xprint("      <crtime prec=\"2\">%s</crtime>" % xboxtime.fatx2iso8601time(fi.fr.ctime, fi.fr.cdate))
                self.xprint("      <atime prec=\"2\">%s</atime>" % xboxtime.fatx2iso8601time(fi.fr.atime, fi.fr.adate))
#self.xprint("      <libmagic>data </libmagic>")
                if not fi.clusters:
                    part.initialize_cluster_list(fi)
                if fi.clusters:
                    self.xprint("      <byte_runs>")
                    cluster_file_offset = 0
                    md5obj = hashlib.md5()
                    sha1obj = hashlib.sha1()

                    last_cluster_length = fi.fr.fsize % (512 * part.sectors_per_cluster)
                    if last_cluster_length == 0:
                        #Account for full last cluster
                        last_cluster_length = 512 * part.sectors_per_cluster
                    for (cluster_no, cluster) in enumerate(fi.clusters):
                        #Build byte runs
                        cluster_fs_offset = (cluster - 1) * part.sectors_per_cluster * 512 + part.root_dir
                        cluster_img_offset = cluster_fs_offset + part.image_offset
                        if cluster_no+1 == len(fi.clusters):
                            cluster_length = last_cluster_length
                        else:
                            cluster_length = 512 * part.sectors_per_cluster
                        self.xprint("        <byte_run py360:cluster='%d' file_offset='%d' fs_offset='%d' img_offset='%d' len='%d' />" % (cluster, cluster_file_offset, cluster_fs_offset, cluster_img_offset, cluster_length))
                        cluster_file_offset += cluster_length

                        #While we're looping, start building hashes as well
                        #Be mindful of the 0 cluster, likely to appear in unallocated files.
                        if cluster != 0:
                            cluster_data = part.read_cluster(cluster, cluster_length)
                            md5obj.update(cluster_data)
                            sha1obj.update(cluster_data)
                    self.xprint("      </byte_runs>")
                    self.xprint("      <hashdigest type='md5'>%s</hashdigest>" % md5obj.hexdigest())
                    self.xprint("      <hashdigest type='sha1'>%s</hashdigest>" % sha1obj.hexdigest())
                self.output("File: %s\t%d" % (filename, fi.fr.fsize))
                self.output("%s\t%s\t%s\n" % (time.ctime(xboxtime.fat2unixtime(fi.fr.mtime, fi.fr.mdate)),\
                                            time.ctime(xboxtime.fat2unixtime(fi.fr.atime, fi.fr.adate)),\
                                            time.ctime(xboxtime.fat2unixtime(fi.fr.ctime, fi.fr.cdate))))
            self.xprint("    </fileobject>")
        self.xprint("""  </volume>""")
                                            
    def print_stfs(self, stf):
        """ Prints out information contained in the provided STFS object """
        self.output("\n*********************")
        self.output(stf)
        self.output("*********************")
        #TODO: Include some of the header data
        self.output("Name: %s" % str(stf.display_name))
        self.output("Description: %s" % str(stf.display_description))
        self.output("\nFILE LISTING")
        for filename in stf.allfiles:
            fl = stf.allfiles[filename]
            self.output("%s\t%s\t %d\t %s " % (time.ctime(xboxtime.fat2unixtime(fl.utime, fl.udate)),\
                                        time.ctime(xboxtime.fat2unixtime(fl.atime, fl.adate)),\
                                        fl.size, filename))
                                    
    def print_xdbf(self, gpd):
        """ Prints out all the information contained in the provided XDBF object
            TODO: Write images to disk if requested
        """
        self.output("\n*********************")
        self.output(gpd)
        self.output("*********************")
        self.output("Version: %d" % gpd.version)
        self.output("Entries: %d" % gpd.entry_count)
        self.output("Images: %d" % len(gpd.images))
        self.output("Titles: %d" % len(gpd.titles))
        self.output("Strings: %d" % len(gpd.strings))
        self.output("Achievements: %d" % len(gpd.achievements))

        self.output("\nSETTINGS")
        for idnum in gpd.settings:
            sett = gpd.settings[idnum]
            self.output("0x%x %s" % (idnum, str(sett)))

        self.output("\nIMAGES")
        for idnum in gpd.images:
            self.output("Image id 0x%x size: %d" % (idnum, len(gpd.images[idnum])))

        self.output("\nTITLES")
        for idnum in gpd.titles:
            title = gpd.titles[idnum]
            try:
                self.output("0x%x: %s" % (idnum, str(title)))
            except UnicodeEncodeError:
                self.output("0x%s: GPD Title %s %s" % (idnum, title.name.replace('\x00', ''), hex(title.title_id)))
            self.output("Achievements unlocked %d / %d" % (title.achievement_unlocked, title.achievement_count))
            self.output("Gamerscore %d / %d" % (title.gamerscore_unlocked, title.gamerscore_total))
            self.output("Last Played: %s\n" % (time.ctime(xboxtime.filetime2unixtime(title.last_played))))

        self.output("\nSTRINGS")
        for idnum in gpd.strings:
            self.output("String id 0x%x" % idnum)
            try:
                self.output("String: %s" % (unicode(gpd.strings[idnum], 'utf-16-be', "ignore")))
            except UnicodeEncodeError:
                self.output("String: %s" % (gpd.strings[idnum].replace('\x00', '')))

        self.output("\nACHIEVEMENTS")
        for idnum in gpd.achievements:
            ach = gpd.achievements[idnum]
            if ach.achievement_id == None or ach.name == None or ach.image_id == None:
                continue
            try:
                self.output("0x%x: %s" % (idnum, str(ach)))
            except UnicodeEncodeError:
                self.output("0x%x: %s %s %s" % (idnum, "GPD Achievement", hex(ach.achievement_id), ach.name.replace('\x00', '')))
            try:
                self.output("Locked Description: %s" % (ach.get_locked_desc()))
                self.output("Unlocked Description: %s" % (ach.get_unlocked_desc()))
            except UnicodeEncodeError:
                self.output("Locked Description: %s" % (ach.locked_desc.replace('\x00', '')))
                self.output("Unlocked Description: %s" % (ach.unlocked_desc.replace('\x00', '')))
                #self.output("Locked Description: %s" % "".join([x for x in ach.get_locked_desc() if x >= 0x20 and x < 0x7F])
                #self.output("Unlocked Description: %s" % "".join([x for x in ach.get_unlocked_desc() if x >= 0x20 and x < 0x7F])

            self.output("Image ID: 0x%x" % ach.image_id)
            self.output("Gamerscore: %d" % ach.gamer_score)
            if ach.unlock_time == 0:
                self.output("Not Unlocked")
            else:
                self.output("Unlocked time: %s" % (time.ctime(xboxtime.filetime2unixtime(ach.unlock_time))))
            self.output(" ")


    def document_image(self):
        """
            Processes an XTAF image including STFS files and embedded GPD and Account files
        """

        if self.filename == None:
            return

        self.xprint("""<?xml version='1.0' encoding='UTF-8'?>
<dfxml version='1.0'>
  <metadata 
  xmlns='http://www.forensicswiki.org/wiki/Category:Digital_Forensics_XML'
  xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' 
  xmlns:dc='http://purl.org/dc/elements/1.1/'>
    <dc:type>Disk Image</dc:type>
  </metadata>
  <creator version='1.0'>
    <program>py360</program>
    <version>""" + __version__ + """</version>
    <build_environment>
      <compiler>python v""" + sys.version.split(" ")[0] + """</compiler>
    </build_environment>
    <execution_environment>""")

        TODO = """
      <os_sysname>Darwin</os_sysname>
      <os_release>11.4.0</os_release>
      <os_version>Darwin Kernel Version 11.4.0: Mon Apr  9 19:32:15 PDT 2012; root:xnu-1699.26.8~1/RELEASE_X86_64</os_version>
      <host>paws.local</host>
      <arch>x86_64</arch>
      <command_line>fiwalk -X/Users/alex/Documents/School/UCSC/SSRC/svn/forensics/src/geoproc.git/results-test/Users/alex/corpus/available/honeynet-scan29.aff/make_fiwalk_dfxml.sh/fiout.xml -f /Users/alex/corpus/available/honeynet-scan29.aff -G0</command_line>
      <start_time>2012-09-03T01:09:15Z</start_time>"""

        self.xprint("""    </execution_environment>
  </creator>
  <source>
    <image_filename>""" + self.filename + """</image_filename>
  </source>""")
        TODO = """  <pagesize>16777216</pagesize>"""
        self.xprint("  <sectorsize>512</sectorsize>")

        self.output("Opening %s" % self.filename, self.errfd)
        #Loop through all known partitions
        for part_offset in [
          0x80000L,
          #0x80080000L, #Encrypted partition
          0x10C080000L,
          0x118EB0000L,
          0x120EB0000L,
          0x130EB0000L
        ]:
          x = partition.Partition(self.filename, part_offset)
          self.print_xtaf(x)

          # Find STFS files
          self.output("Processing all files", self.errfd)
          for filename in x.allfiles:
            try:
                if xboxmagic.find_type(data = x.read_file(filename, size=0x10)) == "STFS":
                    self.output("Processing STFS file %s" % filename, self.errfd)
                    s = stfs.STFS(filename, fd=x.open_fd(filename))
                    self.print_stfs(s)
                    
                    # Check to see if this is a gamertag STFS  
                    for stfsfile in s.allfiles:
                        try:
                            if stfsfile.endswith("Account"):
                                magic = xboxmagic.find_type(data = s.read_file(s.allfiles[stfsfile], size=404))
                            elif stfsfile.upper().endswith(("PNG", "GPD")): 
                                magic = xboxmagic.find_type(data = s.read_file(s.allfiles[stfsfile], size=0x10))
                            else:
                                magic = 'Unknown'

                            # Process GPD files
                            if magic == 'XDBF':
                                self.output("Processing GPD File %s" % stfsfile, self.errfd)
                                # Maybe STFS needs an equivalent to Partition.open_fd(filename)
                                g = xdbf.XDBF(stfsfile, fd=StringIO(s.read_file(s.allfiles[stfsfile])))
                                self.print_xdbf(g)
                                if self.image_directory != None: # Extract all the images
                                    for gpdimage in g.images:
                                        with open("%s/%s-%x-%s" %\
                                                (self.image_directory, os.path.basename(filename), gpdimage,\
                                                stfsfile[1:].replace('/', '-')), 'w') as fd:
                                            fd.write(g.images[gpdimage])
                                        
                            # Decrypt and print Account blob                       
                            if magic == 'Account':
                                self.output("Processing Account Blob", self.errfd)
                                a = account.Account(s.read_file(s.allfiles[stfsfile]))
                                self.print_account(a)
                            
                            # Extract all the images
                            if magic == 'PNG' and self.image_directory != None:
                                self.output("Processing Image File %s" % stfsfile, self.errfd)  
                                with open("%s/%s-%s.png" %\
                                        (self.image_directory, os.path.basename(filename), stfsfile[1:].replace('/', '-')),\
                                        'w') as fd:
                                    fd.write(s.read_file(s.allfiles[stfsfile]))
                        except (IOError, OverflowError, AssertionError) as e: # GPD / Account error
                            self.output("GPD/Account Error: %s %s %s" % (stfsfile, type(e), e), self.errfd)
                            continue

            except (IOError, OverflowError, AssertionError) as e: # STFS Error
                stfs_err_string = "STFS Error: %s %s %s" % (filename, type(e), e)
                self.xprint("  <!--" + stfs_err_string + "-->")
                self.output(stfs_err_string, self.errfd)
                continue
        TODO = """
  <runstats>
    <user_seconds>11</user_seconds>
    <system_seconds>50</system_seconds>
    <maxrss>468721664</maxrss>
    <reclaims>152933</reclaims>
    <faults>2</faults>
    <swaps>0</swaps>
    <inputs>0</inputs>
    <outputs>229</outputs>
    <stop_time>Sun Sep  2 18:15:49 2012</stop_time>
  </runstats>
"""
        self.xprint("</dfxml>")
 
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("xtaf_image", metavar="XTAFIMAGE.bin")
    parser.add_argument("file_output_path", metavar="[path to write images to]", default=None, nargs="?")
    #TODO Implement -X flag like Fiwalk
    parser.add_argument("-x", "--xml", help="Output DFXML (outputs to py360.dfxml)", action="store_true")
    args = parser.parse_args()

    if args.file_output_path:
        reporter = Report360(args.xtaf_image, args.file_output_path)
    else:
        reporter = Report360(args.xtaf_image)
    reporter.init_dfxml(args.xml)

    reporter.document_image()
