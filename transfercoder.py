#!/usr/bin/env python

try:
    import plac
    from placsupport import argument_error
    from placsupport.types import *
except ImportError, e:
    print "You must install the plac and placsupport modules to use this script."
    raise e

import os
import os.path
from subprocess import call
import shutil
from warnings import warn

import os, sys, re, UserDict
from warnings import warn
from itertools import *
import quodlibet.config
quodlibet.config.init()
from quodlibet.formats import MusicFile

def call_silent(cmd, *args, **kwargs):
    """Like subprocess.call, but redirects stdin/out/err to null device."""
    nullsrc = open(os.devnull, "r")
    nullsink = open(os.devnull, "w")
    return call(cmd, *args, stdin=nullsrc, stdout=nullsink, stderr=nullsink, **kwargs)

def test_executable(exe, options=("--help",)):
    """Test whether exe can be executed by doing `exe --help` or similar.

    Returns True if the command succeeds, false otherwise. The exe's
    stdin, stdout, and stderr are all redirected from/to the null
    device."""
    cmd = (exe, ) + options
    try:
        retval = call_silent(cmd)
    except:
        return False
    return retval == 0

def filter_hidden(paths):
    return filter(lambda x: x[0] == ".", paths)

def splitext_afterdot(path):
    """Same as os.path.splitext, but the dot goes to the base."""
    base, ext = os.path.splitext(path)
    if ext[0] = ".":
        base += "."
        ext = ext[1:]
    return (base, ext)

class AudioFile(UserDict.DictMixin):
    """A simple class just for tag editing.

    No internal mutagen tags are exposed, or filenames or anything. So
    calling clear() won't destroy the filename field or things like
    that. Use it like a dict, then .write() it to commit the changes.

    Optional argument blacklist is a list of regexps matching
    non-transferrable tags. They will effectively be hidden, nether
    settable nor gettable.

    Or grab the actual underlying quodlibet format object from the
    .data field and get your hands dirty."""
    def __init__(self, filename, blacklist=()):
        self.data = MusicFile(filename)
        # Also exclude mutagen's internal tags
        self.blacklist = [ re.compile("^~") ] + blacklist
    def __getitem__(self, item):
        if self.blacklisted(item):
            warn("%s is a blacklisted key." % item)
        else:
            return self.data.__getitem__(item)
    def __setitem__(self, item, value):
        if self.blacklisted(item):
            warn("%s is a blacklisted key." % item)
        else:
            return self.data.__setitem__(item, value)
    def __delitem__(self, item):
        if self.blacklisted(item):
            warn("%s is a blacklisted key." % item)
        else:
            return self.data.__delitem__(item)
    def blacklisted(self, item):
        """Return True if tag is blacklisted.

        Blacklist automatically includes internal mutagen tags (those
        beginning with a tilde)."""
        for regex in self.blacklist:
            if re.search(regex, item):
                return True
        else:
            return False
    def keys(self):
        return [ key for key in self.data.keys() if not self.blacklisted(key) ]
    def write(self):
        return self.data.write()

# A list of regexps matching non-transferrable tags, like file format
# info and replaygain info. This will not be transferred from source,
# nor deleted from destination.
blacklist_regexes = [ re.compile(s) for s in (
        'encoded',
        'replaygain',
        ) ]

def copy_tags (src, dest):
    """Replace tags of dest file with those of src.

Excludes format-specific tags and replaygain info, which does not
carry across formats."""
    m_src = AudioFile(src, blacklist = blacklist_regexes)
    m_dest = AudioFile(dest, blacklist = m_src.blacklist)
    m_dest.clear()
    m_dest.update(m_src)
    m_dest.write()

class Transfercode(object):
    def __init__(self, src, dest):
        self.src = src
        self.dest = dest
        self.src_dir = os.path.split(self.src)[0]
        self.dest_dir = os.path.split(self.dest)[0]
        self.src_ext = splitext_afterdot(self.src)[1]
        self.dest_ext = splitext_afterdot(self.dest)[1]

    def needs_update(self):
        """Returns true if dest file needs update.

        This is true when the dest file does not exist, or exists but
        is older than the source file."""
        if not os.path.exists(self.dest):
            return True
        src_mtime = os.path.getmtime(self.src
        dest_mtime = os.path.getmtime(self.dest
        return src_mtime > dest_mtime

    def needs_transcode(self):
        return self.src_ext != self.dest_ext

    def transcode(self, pacpl="pacpl"):
        command = [pacpl, "--overwrite", "--keep", "--to", self.dest_ext, "--outfile", self.dest, self.src]
        call(command)
        copy_tags(self.src, self.dest)

    def copy(self, rsync=None):
        """Copy src to dest.

        Optional arg rsync allows rsync to be used, which may be more
        efficient."""
        success = False
        if rsync:
            try:
                retval = call([ rsync, "-q", "-p", self.src(), self.dest() ]) == 0
                success = (retval == 0)
            except:
                success = False
        if not success:
            # Try regular copy instead if rsync failed
            shutil.copyfile(self.src(), self.dest())
            shutil.copymode(self.src(), self.dest())

    def check(self):
        """Checks that source file and dest dir exist.

        Throws IOError if not. This is called just before initiating
        transfer."""
        if not os.path.isfile(self.src):
            raise IOError("Missing input file: %s" % self.src)
        elif not os.path.isdir(self.dest_dir):
            raise IOError("Missing output directory: %s" % self.dest_dir)

    def transfer(self, pacpl="pacpl", rsync=None):
        """Copies or transcodes src to dest.

    Destination directory must already exist."""
        self.check()
        if self.needs_transcode():
            self.transcode(pacpl=pacpl)
        else:
            self.copy(rsync=rsync)

def is_subpath(path, parent):
    """Returns true if path is a subpath of parent.

    For example, '/usr/bin/python' is a subpath of '/usr', while
    '/bin/ls' is not. Both paths must be absolute, but no checking is
    done. Behavior on relative paths is undefined.

    Any path is a subpath of itself."""
    # Any relative path that doesn't start with ".." is a subpath.
    return os.path.relpath(path, parent)[0:2] != os.path.pardir

class DestinationFinder(object):
    """A class for converting source paths to destination paths."""
    def __init__(self, src_dir, dest_dir, src_exts, dest_ext):
        self.src_dir = os.path.realpath(src_dir)
        self.dest_dir = os.path.realpath(dest_dir)
        # These need leading dots
        self.src_exts = src_exts
        self.dest_ext = dest_ext

    def find_dest(self, src):
        """Returns the absolute destination path for a source path.

        If the source path is absolute, it must lie inside src_dir, or
        ValueError is thrown. A non-absolute path is assumed to be
        relative to src_dir."""
        # Make src relative
        if os.path.isabs(src):
            if not is_subpath(src, self.src_dir):
                raise ValueError("Absolute path must fall within src_dir")
            src = os.path.relpath(src, self.src_dir)
        base, ext = splitext_afterdot(src)
        if ext in self.src_exts:
            dest_relpath = base + self.dest_ext
        else:
            dest_relpath = src
        return os.path.join(self.dest_dir, dest_relpath)

    def transfercodes(self, hidden=False):
        """Generate Transfercode objects for all src files."""
        for root, dirs, files in os.walk(self.src_dir):
            if not hidden:
                dirs = filter_hidden(dirs)
                files = filter_hidden(files)
            for f in files:
                src = os.path.join(root, f)
                dest = self.find_dest(src_path)
                yield Transfercode(src, dest)

def create_dirs(dirs):
    """Ensure that a list of directories all exist"""
    for d in dirs:
        if not os.path.isdir(d):
            os.makedirs(d)

# Plac types
def comma_delimited_set(x):
    # Handles stripping spaces and eliminating zero-length items
    return set(filter(len, list(x.strip() for x in x.split(","))))

def directory(x):
    """Resolve symlinks, then return the result if it is a directory.

    Otherwise throw an error."""
    path = os.path.realpath(x)
    if not os.path.isdir(path):
        if path == x:
            msg = "Not a directory: %s" % x
        else:
            msg = "Not a directory: %s -> %s" % (x, path)
        raise TypeError(msg)
    else:
        return path

# Entry point
def plac_call_main():
    return plac.call(main)

@plac.annotations(
    # arg=(helptext, kind, abbrev, type, choices, metavar)
    source_directory=("The directory with all your music in it.", "positional", directory),
    destination_directory=("The directory where output files will go. The directory hierarchy of the source directory will be replicated here.", "positional", directory),
    transcode_formats=("A comma-separated list of file extensions that must be transcoded.", "option", comma_delimited_set),
    target_format=("All transcode formats will be transcoded to this format.", "option", str),
    pacpl_path=("The path to the Perl Audio Converter. Only required if PAC is not already in your $PATH.", "option", str),
    rsync_path=("The path to the rsync binary. Rsync will be used if available, but it is not required.", "option", str),
    dry_run=("Don't actually modify anything.", "flag"),
    include_hidden=("Don't skip directories and files starting with a dot.", "flag"),
    force=("Update destination files even if they are newer.", "flag"),
    )
def main(source_directory, destination_directory,
         transcode_formats=set("flac", "wv", "wav", "ape", "fla"),
         target_format="ogg",
         pacpl_path=None, rsync_path=None,
         dry_run=False, include_hidden=False, force=False):
    """Mirror a directory with transcoding.

    Everything in the source directory is copied to the destination,
    except that any files of the specified transcode formats are
    transcoded into the target format use Perl Audio Converter. All other
    files are copied over unchanged.

    By default, lossless audio formats are transcoded to ogg, and all
    other formats are copied unmodified. """
    if dry_run:
        print "Running in --dry_run mode. Nothing actually happens."

    if target_format in transcode_formats:
        argument_error('The target format must not be one of the transcode formats')
    pacpl = pacpl_path or "pacpl"
    rsync = rsync_path or "rsync"
    source_directory = os.path.realpath(source_directory)
    destination_directory = os.path.realpath(destination_directory)
    df = DestinationFinder(source_directory, destination_directory,
                           transcode_formats, target_format)
    transfercodes = list(df.transfercodes(hidden=include_hidden))
    if not force:
        transfercodes = filter(lambda x: x.needs_update(), transfercodes)
    if not dry_run:
        create_dirs(set(x.dest_dir for x in transfercodes))
    for tfc in transfercodes:
        print "%s -> %s" % (tfc.src, tfc.dest)
        if not dry_run:
            tfc.transfer()
    print "Done"
    if dry_run:
        print "Ran in --dry_run mode. Nothing actually happened."


if __name__ == "__main__":
    plac_call_main()
