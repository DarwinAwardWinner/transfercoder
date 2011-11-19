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
import tempfile
import multiprocessing

# Fix keyboard interrupts when using multiprocessing.pool.imap().
# https://gist.github.com/626518
from multiprocessing.pool import IMapIterator
def wrapper(func):
  def wrap(self, timeout=None):
    # Note: the timeout of 1 googol seconds introduces a rather subtle
    # bug for Python scripts intended to run many times the age of the universe.
    return func(self, timeout=timeout if timeout is not None else 1e100)
  return wrap
IMapIterator.next = wrapper(IMapIterator.next)

def default_job_count():
    try:
        return multiprocessing.cpu_count()
    except:
        return 1


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
    return filter(lambda x: x[0] != ".", paths)

def splitext_afterdot(path):
    """Same as os.path.splitext, but the dot goes to the base."""
    base, ext = os.path.splitext(path)
    if len(ext) > 0 and ext[0] == ".":
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

    def __repr__(self):
        return "Transfercode(" + repr(self.src) + ', ' + repr(self.dest) + ")"

    def __str__(self):
        return self.__repr__()

    def needs_update(self):
        """Returns true if dest file needs update.

        This is true when the dest file does not exist, or exists but
        is older than the source file."""
        if not os.path.exists(self.dest):
            return True
        src_mtime = os.path.getmtime(self.src)
        dest_mtime = os.path.getmtime(self.dest)
        return src_mtime > dest_mtime

    def needs_transcode(self):
        return self.src_ext != self.dest_ext

    def transcode(self, pacpl="pacpl"):
        # pacpl expects a relative path with no extension, apparently
        rel_dest = os.path.relpath(self.dest, self.src_dir)
        rel_dest_base = os.path.splitext(rel_dest)[0]
        command = [pacpl, "--overwrite", "--keep", "--to", self.dest_ext, "--outfile", rel_dest_base, self.src]
        if call_silent(command) != 0:
            raise Exception("External command failed")
        copy_tags(self.src, self.dest)
        shutil.copymode(self.src, self.dest)

    def copy(self, rsync=None):
        """Copy src to dest.

        Optional arg rsync allows rsync to be used, which may be more
        efficient."""
        success = False
        if rsync is True:
            rsync = "rsync"
        if rsync:
            try:
                retval = call_silent([ rsync, "-q", "-p", self.src, self.dest ]) == 0
                success = (retval == 0)
            except:
                success = False
        if not success:
            # Try regular copy instead if rsync failed
            shutil.copyfile(self.src, self.dest)
            shutil.copymode(self.src, self.dest)

    def check(self):
        """Checks that source file and dest dir exist.

        Throws IOError if not. This is called just before initiating
        transfer."""
        if not os.path.isfile(self.src):
            raise IOError("Missing input file: %s" % self.src)
        elif not os.path.isdir(self.dest_dir):
            raise IOError("Missing output directory: %s" % self.dest_dir)

    def transfer(self, pacpl="pacpl", rsync=None, force=False, report=False, dry_run=False):
        """Copies or transcodes src to dest.

    Destination directory must already exist.

    Optional arg force performs the transfer even if dest is newer.
    Optional arg report prints a description of what is happening.
    Optional arg dry_run skips the actual action."""
        if force or self.needs_update():
            if not dry_run:
                self.check()
            if self.needs_transcode():
                if report:
                    print "Transcoding: %s -> %s" % (self.src, self.dest)
                if not dry_run:
                    self.transcode(pacpl=pacpl)
            else:
                if report:
                    print "Transfering: %s -> %s" % (self.src, self.dest)
                if not dry_run:
                    self.copy(rsync=rsync)
        else:
            if report:
                print "Skipping: %s -> %s" % (self.src, self.dest)


    def transcode_to_tempdir(self, tempdir=None, force=True, *args, **kwargs):
        """Transcode a file to a tempdir.

        Returns a Transfercode object for copying the transcoded temp file to
        the destination.

        If the file does not require transcoding, the input object is returned
        unmodified.

        Extra args are passed to transfer."""
        if self.needs_transcode():
            tempname = os.path.split(self.dest)[1]
            temp_basename, temp_ext = os.path.splitext(tempname)
            temp_filename = tempfile.mkstemp(prefix=temp_basename + "_", suffix=temp_ext, dir=tempdir)[1]
            Transfercode(self.src, temp_filename).transfer(force=True, *args, **kwargs)
            return TransfercodeTemp(temp_filename, self.dest)
        else:
            return self

class TransfercodeTemp(Transfercode):
    """Transfercode subclass where source is a temp file.

The only addition is that the source file is deleted after transfer."""
    def transfer(self, *args, **kwargs):
        super(TransfercodeTemp, self).transfer(*args, **kwargs)
        os.remove(self.src)

    def transcode_to_tempdir(self, *args, **kwargs):
        """This would be redundant."""
        return self

    def __repr__(self):
        return "TransfercodeTemp(" + repr(self.src) + ', ' + repr(self.dest) + ")"

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
                dest = self.find_dest(src)
                yield Transfercode(src, dest)

def create_dirs(dirs):
    """Ensure that a list of directories all exist"""
    for d in dirs:
        if not os.path.isdir(d):
            os.makedirs(d)

class TempdirTranscoder(object):
    """A serializable wrapper for Transfercode.transcode_to_tempdir"""
    def __init__(self, tempdir, pacpl, rsync, force, report):
        self.tempdir = tempdir
        self.pacpl = pacpl
        self.rsync = rsync
        self.force = force
        self.report = report
    def __call__(self, tfc):
        return tfc.transcode_to_tempdir(tempdir=self.tempdir, pacpl=self.pacpl, rsync=self.rsync, force=self.force, report=self.report)

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

def potential_directory(x):
    if os.path.exists(x):
        return directory(x)
    else:
        return x

# Entry point
def plac_call_main():
    return plac.call(main)

@plac.annotations(
    # arg=(helptext, kind, abbrev, type, choices, metavar)
    source_directory=("The directory with all your music in it.", "positional", None, directory),
    destination_directory=("The directory where output files will go. The directory hierarchy of the source directory will be replicated here.", "positional", None, potential_directory),
    transcode_formats=("A comma-separated list of input file extensions that must be transcoded.", "option", "i", comma_delimited_set, None, 'flac,wv,wav,ape,fla'),
    target_format=("All input transcode formats will be transcoded to this output format.", "option", "o", str),
    pacpl_path=("The path to the Perl Audio Converter. Only required if PAC is not already in your $PATH or is installed with a non-standard name.", "option", "p", str),
    rsync_path=("The path to the rsync binary. Rsync will be used if available, but it is not required.", "option", "r", str),
    dry_run=("Don't actually modify anything.", "flag", "m"),
    include_hidden=("Don't skip directories and files starting with a dot.", "flag", "z"),
    force=("Update destination files even if they are newer.", "flag", "f"),
    temp_dir=("Temporary directory to use for transcoded files.", "option", "t", directory),
    jobs=("Number of transcoding jobs to run in parallel. Transfers will always run sequentially. The default is the number of cores available on the system. Use -j1 to force full sequential operation.", "option", "j", positive_int),
    )
def main(source_directory, destination_directory,
         transcode_formats=set(("flac", "wv", "wav", "ape", "fla")),
         target_format="ogg",
         pacpl_path="pacpl", rsync_path="rsync",
         dry_run=False, include_hidden=False, force=False,
         temp_dir=tempfile.gettempdir(), jobs=default_job_count()):
    """Mirror a directory with transcoding.

    Everything in the source directory is copied to the destination,
    except that any files of the specified transcode formats are
    transcoded into the target format use Perl Audio Converter. All other
    files are copied over unchanged.

    By default, lossless audio formats are transcoded to ogg, and all
    other formats are copied unmodified. """
    if dry_run:
        print "Running in --dry_run mode. Nothing actually happens."
        # No point doing nothing in parallel
        jobs = 1

    if target_format in transcode_formats:
        argument_error('The target format must not be one of the transcode formats')
    source_directory = os.path.realpath(source_directory)
    destination_directory = os.path.realpath(destination_directory)
    df = DestinationFinder(source_directory, destination_directory,
                           transcode_formats, target_format)
    transfercodes = list(df.transfercodes(hidden=include_hidden))
    if not dry_run:
        create_dirs(set(x.dest_dir for x in transfercodes))

    if jobs == 1:
        for tfc in transfercodes:
            tfc.transfer(pacpl=pacpl_path, rsync=rsync_path, force=force, report=True, dry_run=dry_run)
    else:
        assert not dry_run, "Parallel dry run makes no sense"
        work_dir = None
        transcode_pool = None
        last_file = None
        try:
            if not force:
                for tfc in ifilter(lambda x: not x.needs_update(), transfercodes):
                    # Handle reporting of skipped files
                    tfc.transfer(report=True)
                transfercodes = filter(lambda x: x.needs_update(), transfercodes)
            if len(transfercodes) > 0:
                work_dir = tempfile.mkdtemp(dir=temp_dir, prefix="transfercode_")
                f = TempdirTranscoder(tempdir=work_dir, pacpl=pacpl_path, rsync=rsync_path,
                                      force=force, report=True)
                transcode_pool = multiprocessing.Pool(jobs)
                transcoded = transcode_pool.imap_unordered(f, transfercodes)
                for tfc in transcoded:
                    last_file = tfc.dest
                    tfc.transfer(pacpl=pacpl_path, rsync=rsync_path, force=force, report=True)
                last_file = None
        except KeyboardInterrupt:
            if transcode_pool is not None:
                transcode_pool.terminate()
            print "\nCanceled."
        finally:
            if transcode_pool is not None:
                transcode_pool.close()
            if work_dir is not None:
                shutil.rmtree(work_dir, ignore_errors=True)
            if last_file is not None:
                print "Cleaning incomplete transfer: %s" % last_file
                os.remove(last_file)
    print "Done."
    if dry_run:
        print "Ran in --dry_run mode. Nothing actually happened."

if __name__ == "__main__":
    plac_call_main()
