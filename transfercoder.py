#!/usr/bin/env python

from warnings import warn
try:
    import plac
except ImportError, e:
    warn("You must install the plac module to use this script.")
    raise e

import UserDict
import hashlib
import logging
import multiprocessing
import os
import os.path
import re
import shlex
import shutil
import sys
import tempfile

from itertools import *
from multiprocessing.pool import ThreadPool
from mutagen import File as MusicFile
from mutagen.aac import AACError
from mutagen.easyid3 import EasyID3
from mutagen.easymp4 import EasyMP4Tags
from subprocess import call, check_call

# Support checksums for MP3 and M4A/MP4
EasyID3.RegisterTXXXKey('transfercoder_src_checksum',
                        'TRANSFERCODER_SRC_CHECKSUM')
EasyMP4Tags.RegisterFreeformKey('transfercoder_src_checksum',
                                'Transfercoder Source Checksum')

def default_job_count():
    try:
        return multiprocessing.cpu_count()
    except Exception:
        return 1

def call_silent(cmd, *args, **kwargs):
    """Like subprocess.call, but redirects stdin/out/err to null device."""
    nullsrc = open(os.devnull, "r")
    nullsink = open(os.devnull, "w")
    logging.debug("Calling command: %s", repr(cmd))
    return call(cmd, *args, stdin=nullsrc, stdout=nullsink, stderr=nullsink, **kwargs)

def test_executable(exe, options=("--help",)):
    """Test whether exe can be executed by doing `exe --help` or similar.

    Returns True if the command succeeds, false otherwise. The exe's
    stdin, stdout, and stderr are all redirected from/to the null
    device."""
    cmd = (exe, ) + options
    try:
        retval = call_silent(cmd)
    except Exception:
        return False
    return retval == 0

def del_hidden(paths):
    """Remove hidden paths from list of paths

This modifies its argument *in place*, so it can be used with
os.walk."""

    hidden = (i for i in reversed(xrange(len(paths)))
              if paths[i][0] == ".")
    for i in hidden:
        del paths[i]

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
    When saving, tags that cannot be saved by the file format will be
    skipped with a debug message, since this is a common occurrance
    with MP3/M4A.

    Optional argument blacklist is a list of regexps matching
    non-transferrable tags. They will effectively be hidden, nether
    settable nor gettable.

    Or grab the actual underlying mutagen format object from the
    .data field and get your hands dirty.

    """
    def __init__(self, filename, blacklist=[], easy=True):
        self.filename = filename
        self.data = MusicFile(self.filename, easy=easy)
        if self.data is None:
            raise ValueError("Unable to identify %s as a music file" % (repr(filename)))
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
            try:
                return self.data.__setitem__(item, value)
            except KeyError:
                logging.debug("Skipping unsupported tag %s for file type %s",
                              item, type(self.data))
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
        return self.data.save()

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
    try:
        m_src = AudioFile(src, blacklist = blacklist_regexes, easy=True)
        m_dest = AudioFile(dest, blacklist = m_src.blacklist, easy=True)
        m_dest.clear()
        m_dest.update(m_src)
        logging.debug("Added tags to dest file:\n%s",
                      "\n".join("%s: %s" % (k, repr(m_dest[k])) for k in sorted(m_dest.keys())))
        m_dest.write()
    except AACError:
        logging.warn("No tags copied because output format does not support tags: %s", repr(type(m_dest.data)))

def read_checksum_tag(fname):
    afile = AudioFile(fname, easy=True)
    try:
        return afile['transfercoder_src_checksum'][0]
    except KeyError:
        logging.debug("Could not read checksum tag from %s", repr(fname))
        return None

def write_checksum_tag(fname, cksum):
    afile = AudioFile(fname, easy=True)
    try:
        afile['transfercoder_src_checksum'] = cksum
        afile.write()
    except Exception:
        logging.warn("Could not write checksum tag to %s", repr(fname))

class Transfercode(object):
    def __init__(self, src, dest, eopts=None, use_checksum=True):
        self.src = src
        self.dest = dest
        self.src_dir = os.path.split(self.src)[0]
        self.dest_dir = os.path.split(self.dest)[0]
        self.src_ext = splitext_afterdot(self.src)[1]
        self.dest_ext = splitext_afterdot(self.dest)[1]
        self.eopts = eopts
        self.use_checksum = use_checksum
        self._src_checksum = None
        self._saved_checksum = None

    def __repr__(self):
        return "%s(src=%s, dest=%s, epots=%s, use_checksum=%s)" % \
            (type(self).__name__, repr(self.src), repr(self.dest),
             repr(self.eopts), repr(self.use_checksum))

    def __str__(self):
        return repr(self)

    def source_checksum(self):
        if self._src_checksum is None:
            m = hashlib.sha256()
            m.update(open(self.src, 'rb').read())
            # We also hash the encoder options, since if those change,
            # we need to re-transcode.
            m.update(self.eopts or "")
            self._src_checksum = m.hexdigest()[:16]
        return self._src_checksum

    def saved_checksum(self):
        if self._saved_checksum is None:
            self._saved_checksum = read_checksum_tag(self.dest)
            if self._saved_checksum is None:
                # Empty string is still false, but is used to
                # distinguish the fact we tried and failed to read the
                # checksum
                self._saved_checksum = ""
        return self._saved_checksum

    def needs_update(self):
        """Returns True if dest file needs update.

        This is true when the dest file does not exist, or exists but
        is older than the source file."""
        if not os.path.exists(self.dest):
            return True
        if self.use_checksum and self.needs_transcode():
            saved_cksum = self.saved_checksum()
            if saved_cksum:
                src_cksum = self.source_checksum()
                return not saved_cksum == src_cksum
            else:
                logging.warn("No checksum found in destination file %s. Falling back to timestamp check.",
                             repr(self.dest))
        # If we haven't returned by now, we need to check based on the modtimes
        src_mtime = os.path.getmtime(self.src)
        dest_mtime = os.path.getmtime(self.dest)
        return src_mtime > dest_mtime

    def needs_transcode(self):
        """Returns True if source and dest have different formats"""
        return self.src_ext != self.dest_ext

    def transcode(self, ffmpeg="ffmpeg", dry_run=False):
        logging.info("Transcoding: %s -> %s", repr(self.src), repr(self.dest))
        logging.debug("Transcoding: %s", repr(self))
        if dry_run:
            return
        # This has no effect if successful, but will throw an error
        # early if we can't read tags from the source file, rather
        # than only discovering the problem after transcoding.
        AudioFile(self.src)

        # Clear the cached checksum from the dest file, if any
        self._saved_checksum = None
        encoder_opts = []
        if self.eopts:
            encoder_opts = shlex.split(self.eopts)

        command = ["ffmpeg", "-y", "-i" , self.src, "-vn"] + \
                  encoder_opts + [ self.dest]
        logging.debug("Transcode command: %s", repr(command))
        check_call(command, stdout=open(os.devnull, "w"),
                   stderr=open(os.devnull, "w"))
        if not os.path.isfile(self.dest):
            raise Exception("ffmpeg did not produce an output file")
        copy_tags(self.src, self.dest)
        if self.use_checksum:
            logging.debug("Saving checksum to dest file %s: %s", repr(self.dest), self.source_checksum())
            write_checksum_tag(self.dest, self.source_checksum())
        try:
            shutil.copymode(self.src, self.dest)
        except OSError:
            # It's ok if setting the mode fails
            pass

    def copy(self, rsync=None, dry_run=False):
        """Copy src to dest.

        Optional arg rsync allows rsync to be used, which may be more
        efficient."""
        logging.info("Copying: %s -> %s", self.src, self.dest)
        if dry_run:
            return
        success = False
        if rsync is True:
            rsync = "rsync"
        if rsync:
            try:
                retval = call_silent([ rsync, "-q", "-p", self.src, self.dest ]) == 0
                success = (retval == 0)
            except Exception:
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

    def transfer(self, ffmpeg="ffmpeg", rsync=None, force=False, dry_run=False, transcode_tempdir=None):
        """Copies or transcodes src to dest.

    Destination directory must already exist. Optional arg force
    performs the transfer even if no update is required. Optional arg
    transcode_tempdir specifies where to transcode before copying.

    Optional arg dry_run skips the actual action.

        """
        if force or self.needs_update():
            if not dry_run:
                self.check()
            if self.needs_transcode():
                if transcode_tempdir:
                    temp = self.transcode_to_tempdir(ffmpeg=ffmpeg, rsync=rsync, tempdir=transcode_tempdir, force=True, dry_run=dry_run)
                    temp.transfer(ffmpeg=ffmpeg, rsync=rsync, force=force, dry_run=dry_run, transcode_tempdir=None)
                else:
                    self.transcode(ffmpeg=ffmpeg, dry_run=dry_run)
            else:
                self.copy(rsync=rsync, dry_run=dry_run)
        elif self.needs_transcode() and self.use_checksum and not self.saved_checksum():
            logging.info("Saving checksum to destination file")
            write_checksum_tag(self.dest, self.source_checksum())
        else:
            logging.debug("Skipping: %s -> %s", self.src, self.dest)

    # TODO: implement encoder options
    def transcode_to_tempdir(self, tempdir=None, force=False, dry_run=False, *args, **kwargs):
        """Transcode a file to a tempdir.

        Returns a Transfercode object for copying the transcoded temp file to
        the destination.

        If the file does not require transcoding, the input object is
        returned unmodified.

        Extra args are passed to Transfercode.transfer."""
        if dry_run:
            return self
        if not self.needs_transcode():
            return self
        if not (force or self.needs_update()):
            return self
        tempname = os.path.split(self.dest)[1]
        temp_basename, temp_ext = os.path.splitext(tempname)
        temp_filename = tempfile.mkstemp(prefix=temp_basename + "_", suffix=temp_ext, dir=tempdir)[1]
        Transfercode(self.src, temp_filename, self.eopts).transfer(force=True, *args, **kwargs)
        return TransfercodeTemp(temp_filename, self.dest, self.eopts, self.use_checksum)

class TransfercodeTemp(Transfercode):
    """Transfercode subclass where source is a temp file.

The only addition is that the source file is deleted after transfer."""
    def transfer(self, *args, **kwargs):
        super(TransfercodeTemp, self).transfer(*args, **kwargs)
        os.remove(self.src)

    def transcode_to_tempdir(self, *args, **kwargs):
        """This would be redundant."""
        return self

def is_subpath(path, parent):
    """Returns true if path is a subpath of parent.

    For example, '/usr/bin/python' is a subpath of '/usr', while
    '/bin/ls' is not. Both paths must be absolute, but no checking is
    done. Behavior on relative paths is undefined.

    Any path is a subpath of itself."""
    # Any relative path that doesn't start with ".." is a subpath.
    return not os.path.relpath(path, parent)[0:2].startswith(os.path.pardir)

def walk_files(dir, hidden=False):
    """Iterator over paths to non-directory files in dir.

    The returned paths will all start with dir. In particular, if dir
    is absolute, then all returned paths will be absolute.

    If hidden=True, include hidden files and files inside hidden
    directories."""
    for root, dirs, files in os.walk(dir):
        if not hidden:
            del_hidden(dirs)
            del_hidden(files)
        for f in files:
            yield os.path.join(root, f)

class DestinationFinder(object):
    """A class for converting source paths to destination paths."""
    def __init__(self, src_dir, dest_dir, src_exts, dest_ext, hidden=False):
        self.src_dir = os.path.realpath(src_dir)
        self.dest_dir = os.path.realpath(dest_dir)
        # These need leading dots
        self.src_exts = src_exts
        self.dest_ext = dest_ext
        self.include_hidden = hidden

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

    def walk_source_files(self):
        """An iterator over all files in the source directory."""
        return walk_files(self.src_dir, hidden=self.include_hidden)

    def walk_target_files(self):
        """An iterator over all files that are to be created in the destination directory."""
        return imap(self.find_dest, self.walk_source_files())

    def walk_source_target_pairs(self):
        """iter(zip(self.walk_source_files(), self.walk_target_files()))'.

        Only it's more efficient."""
        return ((src, self.find_dest(src)) for src in self.walk_source_files())

    def walk_existing_dest_files(self):
        """An iterator over all existing files in the destination directory."""
        return walk_files(self.dest_dir, hidden=self.include_hidden)

    def walk_extra_dest_files(self):
        """An iterator over all existing files in the destination directory that are not targets of source files.

        These are the files that transfercoder would delete if given the --delete option."""
        target_files = list(self.walk_target_files())
        return sorted(set(self.walk_existing_dest_files()).difference(self.walk_target_files()))

    def transfercodes(self, eopts=None, use_checksum=True):
        """Generate Transfercode objects for all src files.

        Optional arg 'eopts' is passed to the Transfercode() constructor."""
        return (Transfercode(src, dest, eopts, use_checksum) for src, dest in self.walk_source_target_pairs())

def create_dirs(dirs):
    """Ensure that a list of directories all exist"""
    for d in dirs:
        if not os.path.isdir(d):
            logging.debug("Creating directory: %s", d)
            os.makedirs(d)

class ParallelTranscodeException(Exception):
    def __init__(self, message, exc, fname):

        # Call the base class constructor with the parameters it needs
        super(ParallelTranscodeException, self).__init__(message)

        # Now for your custom code...
        self.exc = exc
        self.fname = fname

class TempdirTranscoder(object):
    """A serializable wrapper for Transfercode.transcode_to_tempdir"""
    def __init__(self, tempdir, ffmpeg, rsync, force):
        self.tempdir = tempdir
        self.ffmpeg = ffmpeg
        self.rsync = rsync
        self.force = force
    def __call__(self, tfc):
        try:
            return tfc.transcode_to_tempdir(tempdir=self.tempdir, ffmpeg=self.ffmpeg, rsync=self.rsync, force=self.force)
        except Exception as exc:
            return ParallelTranscodeException(str(exc), exc, tfc.src)

# See:
# https://trac.ffmpeg.org/wiki/Encode/HighQualityAudio
# https://trac.ffmpeg.org/wiki/Encode/AAC
# https://trac.ffmpeg.org/wiki/Encode/MP3
# https://trac.ffmpeg.org/wiki/TheoraVorbisEncodingGuide
# http://blog.codinghorror.com/concluding-the-great-mp3-bitrate-experiment/
# http://wiki.hydrogenaud.io/index.php?title=Transparency
# http://soundexpert.org/encoders-128-kbps
default_eopts = {
    # VBR ~192kbps
    'mp3': '-codec:a libmp3lame -q:a 2',
    # VBR ~160kbps
    'ogg': '-codec:a libvorbis -q:a 5',
    # VBR ~192kbps
    'aac': '-codec:a libfdk_aac -vbr 5',
    'm4a': '-codec:a libfdk_aac -vbr 5',
    'mp4': '-codec:a libfdk_aac -vbr 5',
    # VBR ~160kbps
    'opus': '-codec:a libopus -b:a 160k',
}

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

def nonneg_int(x):
    x = int(x)
    if x < 0:
        raise ValueError("Not a nonnegative integer")
    return x

# Entry point
def plac_call_main():
    return plac.call(main)

@plac.annotations(
    # arg=(helptext, kind, abbrev, type, choices, metavar)
    source_directory=("The directory with all your music in it.", "positional", None, directory),
    destination_directory=("The directory where output files will go. The directory hierarchy of the source directory will be replicated here.", "positional", None, potential_directory),
    transcode_formats=("A comma-separated list of input file extensions that must be transcoded. ffmpeg must be compiled with support for decoding these formats.", "option", "i", comma_delimited_set, None, 'flac,wv,wav,ape,fla'),
    target_format=("All input transcode formats will be transcoded to this output format. ffmpeg must be compiled with support for encoding this format.", "option", "o", str),
    ffmpeg_path=("The path to ffmpeg. Only required if ffmpeg is not already in your $PATH or is installed with a non-standard name.", "option", "p", str),
    encoder_options=("Extra encoder options to pass to ffmpeg.", "option", "E", str, None, "'OPTIONS'"),
    rsync_path=("The path to the rsync binary. Rsync will be used if available, but it is not required.", "option", "r", str),
    dry_run=("Don't actually modify anything.", "flag", "n"),
    include_hidden=("Don't skip directories and files starting with a dot.", "flag", "z"),
    delete=("Delete files in the destination that do not have a corresponding file in the source directory.", "flag", "D"),
    force=("Update destination files even if they are newer.", "flag", "f"),
    no_checksum_tags=("Don't save a checksum of the source file in the destination file. Instead, determine whether to transcode a file based on whether the target file has a newer modification time.", "flag", "k"),
    temp_dir=("Temporary directory to use for transcoded files.", "option", "t", directory),
    jobs=("Number of transcoding jobs to run in parallel. Transfers will always run sequentially. The default is the number of cores available on the system. A value of 1 will run transcoding in parallel with copying. Use -j0 to force full sequential operation.", "option", "j", nonneg_int),
    quiet=("Do not print informational messages.", "flag", "q"),
    verbose=("Print debug messages that are probably only useful if something is going wrong.", "flag", "v"),
    )
def main(source_directory, destination_directory,
         transcode_formats=set(("flac", "wv", "wav", "ape", "fla")),
         target_format="ogg",
         ffmpeg_path="ffmpeg", encoder_options=None,
         rsync_path="rsync",
         dry_run=False, include_hidden=False, delete=False, force=False,
         no_checksum_tags=False,
         quiet=False, verbose=False,
         temp_dir=tempfile.gettempdir(), jobs=default_job_count()):
    """Mirror a directory with transcoding.

    Everything in the source directory is copied to the destination,
    except that any files of the specified transcode formats are
    transcoded into the target format using ffmpeg. All other files
    are copied over unchanged.

    The default behavior is to transcode several lossless formats
    (flac, wavpack, wav, and ape) to ogg, and all other files are
    copied over unmodified.

    """
    if quiet:
        logging.basicConfig(level=logging.WARN)
    elif verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if target_format in transcode_formats:
        argument_error('The target format must not be one of the transcode formats')

    if dry_run:
        logging.info("Running in --dry_run mode. Nothing actually happens.")
        # No point doing nothing in parallel
        if jobs > 0:
            logging.debug("Switching to sequential mode because --dry_run was specified.")
            jobs = 0

    logging.debug("Using %s to determine whether updates are needed.",
                  "file modification times" if no_checksum_tags else "checksum tags")

    source_directory = os.path.realpath(source_directory)
    destination_directory = os.path.realpath(destination_directory)
    df = DestinationFinder(source_directory, destination_directory,
                           transcode_formats, target_format, include_hidden)
    transfercodes = list(df.transfercodes(eopts=encoder_options, use_checksum=not no_checksum_tags))
    need_at_least_one_transcode = any(imap(lambda x: (force or x.needs_update()) and x.needs_transcode(), transfercodes))

    if need_at_least_one_transcode:
        # Only emit encoder-related log messages if transcoding is required
        if encoder_options is None:
            try:
                encoder_options = default_eopts[target_format]
                logging.debug("Using default encoder options for %s format: %s", target_format, repr(encoder_options))
            except KeyError:
                logging.debug("Using default encoder options for %s format", target_format)
    else:
        # Only transcoding happens in parallel, not transferring, so
        # disable parallel if no transcoding is required
        if jobs > 0:
            logging.debug("Switching to sequential mode because no transcodes are required.")
            jobs = 0

    work_dir = tempfile.mkdtemp(dir=temp_dir, prefix="transfercode_")
    try:
        if not dry_run:
            create_dirs(set(x.dest_dir for x in transfercodes))
        failed_files = []
        if jobs == 0:
            logging.debug("Running in sequential mode.")
            for tfc in transfercodes:
                try:
                    fname = tfc.src
                    tfc = tfc.transcode_to_tempdir(tempdir=work_dir, ffmpeg=ffmpeg_path,
                                                   rsync=rsync_path, force=force, dry_run=dry_run)
                except Exception as exc:
                    logging.exception("Exception while transcoding %s: %s", fname, exc)
                    failed_files.append(fname)
                    continue
                try:
                    tfc.transfer(ffmpeg=ffmpeg_path, rsync=rsync_path, force=force, dry_run=dry_run)
                except Exception as exc:
                    logging.exception("Exception while transferring %s: %s", fname, exc)
                    failed_files.append(fname)
                    continue

        else:
            assert not dry_run, "Parallel dry run makes no sense"
            logging.debug("Running %s transcoding %s and 1 transfer job in parallel.", jobs, ("jobs" if jobs > 1 else "job"))
            transcode_pool = None
            last_file = None
            try:
                # Transcoding step (parallel)
                if need_at_least_one_transcode:
                    f = TempdirTranscoder(tempdir=work_dir, ffmpeg=ffmpeg_path, rsync=rsync_path,
                                          force=force)
                    transcode_pool = ThreadPool(jobs)
                    # Sort jobs that don't need transcoding first
                    transfercodes = sorted(transfercodes, key = Transfercode.needs_transcode)
                    transcoded = transcode_pool.imap_unordered(f, transfercodes)
                else:
                    # Skip the transcoding step if nothing needs transcoding
                    transcoded = transfercodes
                # Transfer step (not parallel, since it is disk-bound)
                for tfc in transcoded:
                    if isinstance(tfc, ParallelTranscodeException):
                        pexc = tfc
                        orig_exc = pexc.exc
                        fname = pexc.fname
                        try:
                            raise orig_exc
                        except Exception as exc:
                            logging.exception("Exception while transcoding %s: %s", fname, exc)
                            failed_files.append(fname)
                            continue
                    last_file = tfc.dest
                    try:
                        tfc.transfer(ffmpeg=ffmpeg_path, rsync=rsync_path, force=force, dry_run=dry_run)
                    except Exception as exc:
                        logging.exception("Exception while transferring %s: %s", fname, exc)
                        failed_files.append(fname)
                        continue
                last_file = None
            except KeyboardInterrupt:
                logging.error("Canceled.")
                delete = False
                if transcode_pool is not None:
                    logging.debug("Terminating transcode process pool")
                    transcode_pool.terminate()
                    transcode_pool = None
            finally:
                if transcode_pool is not None:
                    logging.debug("Closing transcode process pool")
                    transcode_pool.close()
                if last_file and os.path.exists(last_file):
                    logging.info("Cleaning incomplete transfer: %s", last_file)
                    os.remove(last_file)
    finally:
        if work_dir and os.path.exists(work_dir):
            logging.debug("Deleting temporary directory")
            shutil.rmtree(work_dir, ignore_errors=True)
    if delete:
        for f in df.walk_extra_dest_files():
            logging.info("Deleting: %s", f)
            if not dry_run:
                os.remove(f)
    if failed_files:
        logging.error("The following %s files were not processed successfully:\n%s",
                      len(failed_files),
                      "\n".join("\t" + f for f in failed_files))
        logging.info("Finished with some errors (see above).")
    else:
        logging.info("Finished with no errors.")
    if dry_run:
        logging.info("Ran in --dry_run mode. Nothing actually happened.")

if __name__ == "__main__":
    plac_call_main()
