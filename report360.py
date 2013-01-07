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
        self.output("\n*********************")
        self.xprint("""  <volume offset="%s">""" % str(part.start))
        self.xprint("  <partition_offset>%s</partition_offset>" % str(part.start))
        self.xprint("  <block_size>512</block_size>")
#TODO
#    <ftype>256</ftype>
        self.xprint("  <ftype_str>xtaf</ftype_str>")
#    <block_count>235516</block_count>
#    <first_block>0</first_block>
#    <last_block>235515</last_block>

        self.output(part)
        self.output("*********************")
        self.output("\nFILE LISTING")
        #for filename in part.allfiles:
        for filename in part.walk():
            fi = part.get_file(filename)
            self.xprint("    <fileobject>")
#TODO
#      <parent_object>
#        <inode>2</inode>
#      </parent_object>
            self.xprint("      <filename>%s</filename>" % (filename))
#      <partition>1</partition>
#      <id>1</id>
            name_type = "-" #TODO This appears in some Fiwalk output for unallocated files
            if fi.isDirectory():
                name_type = "d"
            else:
                name_type = "r"
            self.xprint("      <name_type>d</name_type>")
            if fi.fr:
#TODO
#      <filesize>4096</filesize>
#      <alloc>1</alloc>
#      <used>1</used>
#      <inode>2</inode>
#      <meta_type>2</meta_type>
#      <mode>493</mode>
#      <nlink>18</nlink>
#      <uid>0</uid>
#      <gid>0</gid>
#      <mtime>2003-08-10T22:54:04Z</mtime>
#      <ctime>2003-08-10T22:54:04Z</ctime>
#      <atime>2003-08-10T22:56:11Z</atime>
#      <libmagic>data </libmagic>
#      <byte_runs>
#       <byte_run file_offset='0' fs_offset='1900544' img_offset='1916928' len='4096'/>
#      </byte_runs>
#      <hashdigest type='md5'>95b1da7257ad7bc44a19757d8980b49e</hashdigest>
#      <hashdigest type='sha1'>3ccde64a5035f839ee508d5309f5c49b6a384411</hashdigest>
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
    <version>""" + __version__ + """</version>""")

        TODO = """
    <build_environment>
      <compiler>GCC 4.2</compiler>
      <library name="afflib" version="3.7.1"/>
      <library name="libewf" version="20120504"/>
    </build_environment>
    <execution_environment>
      <os_sysname>Darwin</os_sysname>
      <os_release>11.4.0</os_release>
      <os_version>Darwin Kernel Version 11.4.0: Mon Apr  9 19:32:15 PDT 2012; root:xnu-1699.26.8~1/RELEASE_X86_64</os_version>
      <host>paws.local</host>
      <arch>x86_64</arch>
      <command_line>fiwalk -X/Users/alex/Documents/School/UCSC/SSRC/svn/forensics/src/geoproc.git/results-test/Users/alex/corpus/available/honeynet-scan29.aff/make_fiwalk_dfxml.sh/fiout.xml -f /Users/alex/corpus/available/honeynet-scan29.aff -G0</command_line>
      <start_time>2012-09-03T01:09:15Z</start_time>
    </execution_environment>"""

        self.xprint("""  </creator>
  <source>
    <image_filename>""" + self.filename + """</image_filename>
  </source>""")
        TODO = """  <pagesize>16777216</pagesize>"""
        self.xprint("  <sectorsize>512</sectorsize>")

        self.output("Opening %s" % self.filename, self.errfd)
        x = partition.Partition(self.filename)
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
                self.output("STFS Error: %s %s %s" % (filename, type(e), e), self.errfd)
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
    parser.add_argument("-x", "--xml", help="Output DFXML", action="store_true")
    args = parser.parse_args()

    if args.file_output_path:
        reporter = Report360(args.xtaf_image, args.file_output_path)
    else:
        reporter = Report360(args.xtaf_image)
    reporter.init_dfxml(args.xml)

    reporter.document_image()
