from typing import Any, Dict, Iterable, List, Pattern, Sequence, Tuple, Union

import hashlib
import logging
import os
import os.path
import re
import shlex
import shutil
import tempfile

from ffmpy import FFmpeg, FFRuntimeError
from mutagen import File as MusicFile
from mutagen.aac import AACError
from mutagen.easyid3 import EasyID3
from mutagen.easymp4 import EasyMP4Tags
from subprocess import call, check_call
from rganalysis import RGTrack

try:
    # Python 3
    from collections.abc import MutableMapping
except ImportError:
    # Python 2
    from UserDict import DictMixin as MutableMapping

# Set up logging
logFormatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = []
logger.addHandler(logging.StreamHandler())
for handler in logger.handlers:
    handler.setFormatter(logFormatter)

# Support checksums for MP3 and M4A/MP4
EasyID3.RegisterTXXXKey('transfercoder_src_checksum',
                        'TRANSFERCODER_SRC_CHECKSUM')
EasyMP4Tags.RegisterFreeformKey('transfercoder_src_checksum',
                                'Transfercoder Source Checksum')

def call_silent(cmd: Union[str, Sequence[str]], *args, **kwargs) -> int:
    """Like subprocess.call, but redirects stdin/out/err to null device."""
    nullsrc = open(os.devnull, "r")
    nullsink = open(os.devnull, "w")
    kwargs['stdin'] = nullsrc
    kwargs['stdout'] = nullsink
    kwargs['stderr'] = nullsink
    logger.debug("Calling command: %s", repr(cmd))
    return call(cmd, *args, **kwargs)

def del_hidden(paths: List[str]) -> None:
    """Remove hidden paths from list of paths

This modifies its argument *in place* rather than returning it, so it
can be used with os.walk.

    """

    hidden = (i for i in reversed(range(len(paths)))
              if paths[i][0] == ".")
    for i in hidden:
        del paths[i]

def splitext_afterdot(path: str) -> Tuple[str, str]:
    """Same as os.path.splitext, but the dot goes to the base."""
    base, ext = os.path.splitext(path)
    if len(ext) > 0 and ext[0] == ".":
        base += "."
        ext = ext[1:]
    return (base, ext)

class AudioFile(MutableMapping):
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
    def __init__(self, filename: str, blacklist: List[Pattern[str]]=[],
                 easy: bool=True) -> None:
        self.filename = filename
        self.data = MusicFile(self.filename, easy=easy)
        if self.data is None:
            raise ValueError("Unable to identify %s as a music file" % (repr(filename)))
        # Also exclude mutagen's internal tags
        self.blacklist = [ re.compile("^~") ] + blacklist
    def __getitem__(self, item: str) -> Any:
        if self.blacklisted(item):
            logger.debug("Attempted to get blacklisted key: %s." % repr(item))
        else:
            return self.data.__getitem__(item)
    def __setitem__(self, item: str, value: Any) -> None:
        if self.blacklisted(item):
            logger.debug("Attempted to set blacklisted key: %s." % repr(item))
        else:
            try:
                return self.data.__setitem__(item, value)
            except KeyError:
                logger.debug("Skipping unsupported tag %s for file type %s",
                             item, type(self.data))
    def __delitem__(self, item: str):
        if self.blacklisted(item):
            logger.debug("Attempted to del blacklisted key: %s." % repr(item))
        else:
            return self.data.__delitem__(item)
    def __len__(self) -> int:
        return len(list(self.keys()))
    def __iter__(self) -> Iterable[Any]:
        return iter(self.keys())

    def blacklisted(self, item: str) -> bool:
        """Return True if tag is blacklisted.

        Blacklist automatically includes internal mutagen tags (those
        beginning with a tilde)."""
        for regex in self.blacklist:
            if re.search(regex, item):
                return True
        else:
            return False
    def keys(self) -> Iterable[str]:
        return [ key for key in self.data.keys() if not self.blacklisted(key) ]
    def write(self) -> None:
        return self.data.save()

# A list of regexps matching non-transferrable tags, like file format
# info and replaygain info. This will not be transferred from source,
# nor deleted from destination.
blacklist_regexes = [ re.compile(s) for s in (
    'encoded',
    'replaygain',
) ]

def copy_tags (src: str, dest: str) -> None:
    """Replace tags of dest file with those of src.

Excludes format-specific tags and replaygain info, which does not
carry across formats."""
    try:
        m_src = AudioFile(src, blacklist = blacklist_regexes, easy=True)
        m_dest = AudioFile(dest, blacklist = m_src.blacklist, easy=True)
        m_dest.clear()
        logging.debug("Adding tags from source file:\n%s",
                      "\n".join("%s: %s" % (k, repr(m_src[k])) for k in sorted(m_src.keys())))
        m_dest.update(m_src)
        logger.debug("Added tags to dest file:\n%s",
                     "\n".join("%s: %s" % (k, repr(m_dest[k])) for k in sorted(m_dest.keys())))
        m_dest.write()
    except AACError:
        logger.warn("No tags copied because output format does not support tags: %s", repr(type(m_dest.data)))

def read_checksum_tag(fname: str) -> str:
    try:
        afile = AudioFile(fname, easy=True)
        return afile['transfercoder_src_checksum'][0]
    except Exception:
        logger.debug("Could not read checksum tag from %s", repr(fname))
        return None

def write_checksum_tag(fname: str, cksum: str) -> None:
    try:
        afile = AudioFile(fname, easy=True)
        afile['transfercoder_src_checksum'] = cksum
        afile.write()
    except Exception:
        logger.warn("Could not write checksum tag to %s", repr(fname))

def delete_replaygain_tags(fname: str) -> None:
    '''Delete all ReplayGain tags from a music file.'''
    RGTrack(fname).cleanup_tags()

class Transfercode(object):
    def __init__(self, src: str, dest: str, eopts: str=None, use_checksum: bool=True) -> None:
        self.src = src
        self.dest = dest
        self.src_dir = os.path.split(self.src)[0]
        self.dest_dir = os.path.split(self.dest)[0]
        self.src_ext = splitext_afterdot(self.src)[1]
        self.dest_ext = splitext_afterdot(self.dest)[1]
        self.eopts = eopts
        self.use_checksum = use_checksum
        self._src_checksum = None # type: str
        self._saved_checksum = None # type: str
        self.needs_transcode = self.src_ext != self.dest_ext

    def __repr__(self) -> str:
        return "%s(src=%s, dest=%s, epots=%s, use_checksum=%s)" % \
            (type(self).__name__, repr(self.src), repr(self.dest),
             repr(self.eopts), repr(self.use_checksum))

    def __str__(self) -> str:
        return repr(self)

    def source_checksum(self) -> str:
        if self._src_checksum is None:
            m = hashlib.sha256()
            m.update(open(self.src, 'rb').read())
            # We also hash the encoder options, since if those change,
            # we need to re-transcode.
            m.update((self.eopts or "").encode('utf-8'))
            self._src_checksum = m.hexdigest()[:32]
        return self._src_checksum

    def saved_checksum(self) -> str:
        if self._saved_checksum is None:
            logger.debug("Reading checksum from %s", repr(self.dest))
            self._saved_checksum = read_checksum_tag(self.dest)
            if self._saved_checksum is None:
                # Empty string is still false, but is used to
                # distinguish the fact we tried and failed to read the
                # checksum
                self._saved_checksum = ""
        return self._saved_checksum or None

    def checksum_current(self) -> bool:
        """Returns True, False, or None for no checksum."""
        return self.saved_checksum() is not None and \
            self.saved_checksum() == self.source_checksum()

    def save_checksum(self) -> None:
        # Clear the cached checksum from the dest file, if any
        self._saved_checksum = None
        logger.info("Saving checksum to destination file %s", repr(self.dest))
        write_checksum_tag(self.dest, self.source_checksum())

    def needs_update(self, loglevel: int=logging.DEBUG) -> bool:
        """Returns True if dest file needs update.

        For transcodable files, if self.use_checksum is True, the
        checksum of the source file will be checked against the
        checksum stored in the tags of the destination file. If they
        don't match, this returns True. If self.use_checksum is False
        or the source file does not need to be transcoded, this
        returns True if the source file is newer than the destination.

        By default, log messages explaining the status of each file
        that needs updating will be emitted as debug messages. Pass an
        alternate level for the loglevel argument to change this.
        (This is needed because this method is called multiple times,
        so it would produce multiple redundant messages if not for
        this option.)

        """
        verb = "transcode" if self.needs_transcode else "copy"
        if not os.path.exists(self.dest):
            logger.log(loglevel, "Need to %s file %s to %s because the destination file does not exist yet",
                       verb, repr(self.src), repr(self.dest))
            return True
        # Only use checksums for transcodable files
        if self.needs_transcode and self.use_checksum:
            current = self.checksum_current()
            if current is None:
                logger.log(loglevel,
                           "Expected checksum not found in tags for destination file %s. Falling back to modification time checking.",
                           self.dest)
            else:
                logger.debug("Using checksum to determine update status for %s", repr(self))
                if current:
                    logger.debug("Don't need to transcode %s to %s because the destination's checksum tag matches the source's checksum.",
                                 repr(self.src), repr(self.dest))
                else:
                    logger.log(loglevel, "Need to transcode %s to %s because the destination's checksum tag does not match the source's checksum.",
                               repr(self.src), repr(self.dest))
                return not current
            # If we haven't returned by now, we need to check based on the modtimes
        logger.debug("Using modification times to determine update status for %s", repr(self))
        src_mtime = os.path.getmtime(self.src)
        dest_mtime = os.path.getmtime(self.dest)
        src_newer = src_mtime > dest_mtime
        if src_newer:
            logger.log(loglevel, "Need to %s file %s to %s because the source file is newer than the destination file",
                       verb, repr(self.src), repr(self.dest))
        else:
            logger.debug("Don't to %s file %s to %s because the destination file is newer than the source file",
                         verb, repr(self.src), repr(self.dest))
        return src_newer

    def transcode(self, ffmpeg: str="ffmpeg", dry_run: bool=False,
                  show_ffmpeg_output=False) -> None:
        logger.info("Transcoding: %s -> %s", repr(self.src), repr(self.dest))
        logger.debug("Transcoding: %s", repr(self))
        if dry_run:
            return
        # This has no effect if successful, but will throw an error
        # early if we can't read tags from the source file, rather
        # than only discovering the problem after transcoding.
        AudioFile(self.src)

        encoder_opts = []       # type: List[str]
        if self.eopts:
            encoder_opts = shlex.split(self.eopts)

        ff = FFmpeg(
            executable = ffmpeg,
            global_options = '-y',
            inputs = { self.src: None },
            outputs = { self.dest: ['-vn'] + encoder_opts },
        )
        logger.debug("Transcode command: %s", repr(ff.cmd))
        if show_ffmpeg_output:
            output_target = None
        else:
            output_target = open(os.devnull, "w")
        ff.run(stdout=output_target, stderr=output_target)
        if not os.path.isfile(self.dest):
            raise Exception("ffmpeg did not produce an output file")
        logger.debug("Deleting any ReplayGain tags on destination file %s", repr(self.dest))
        delete_replaygain_tags(self.dest)
        logger.debug("Copying tags from %s to %s", repr(self.src), repr(self.dest))
        copy_tags(self.src, self.dest)
        if self.use_checksum:
            logger.debug("Saving checksum to dest file %s: %s", repr(self.dest), self.source_checksum())
            write_checksum_tag(self.dest, self.source_checksum())
        try:
            shutil.copymode(self.src, self.dest)
        except OSError:
            # It's ok if setting the mode fails
            pass

    def copy(self, rsync: Union[str, bool]=None, dry_run: bool=False):
        """Copy src to dest.

        Optional arg rsync allows rsync to be used, which may be more
        efficient."""
        logger.info("Copying: %s -> %s", self.src, self.dest)
        if dry_run:
            return
        success = False
        if isinstance(rsync, bool):
            if rsync is True:
                rsync = "rsync"
            else:
                rsync = None
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

    def check(self) -> None:
        """Checks that source file and dest dir exist.

        Throws IOError if not. This is called just before initiating
        transfer."""
        if not os.path.isfile(self.src):
            raise IOError("Missing input file: %s" % self.src)
        elif not os.path.isdir(self.dest_dir):
            raise IOError("Missing output directory: %s" % self.dest_dir)

    def transfer(self, ffmpeg: str="ffmpeg", rsync: Union[str, bool]=None,
                 force: bool=False, dry_run: bool=False,
                 transcode_tempdir: str=None,
                 *args, **kwargs) -> None:
        """Copies or transcodes src to dest.

    Destination directory must already exist. Optional arg force
    performs the transfer even if no update is required. Optional arg
    transcode_tempdir specifies where to transcode before copying.

    Optional arg dry_run skips the actual action.

        """
        if force or self.needs_update():
            if not dry_run:
                self.check()
            if self.needs_transcode:
                if transcode_tempdir:
                    temp = self.transcode_to_tempdir(ffmpeg=ffmpeg, rsync=rsync, tempdir=transcode_tempdir, force=True, dry_run=dry_run, *args, **kwargs)
                    temp.transfer(ffmpeg=ffmpeg, rsync=rsync, force=force, dry_run=dry_run, transcode_tempdir=None, *args, **kwargs)
                else:
                    self.transcode(ffmpeg=ffmpeg, dry_run=dry_run, *args, **kwargs)
            else:
                self.copy(rsync=rsync, dry_run=dry_run)
                # If the destination is missing its checksum, we still need to
                # add it even though we're not updating the file.
        elif not dry_run and self.use_checksum and self.needs_transcode and not self.checksum_current():
            self.save_checksum()
        else:
            logger.debug("Skipping: %s -> %s", self.src, self.dest)

    # TODO: implement encoder options
    def transcode_to_tempdir(self, tempdir: str=None, force: bool=False,
                             dry_run: bool=False, *args,
                             **kwargs) -> 'Transfercode':
        """Transcode a file to a tempdir.

        Returns a Transfercode object for copying the transcoded temp file to
        the destination.

        If the file does not require transcoding, the input object is
        returned unmodified.

        Extra args are passed to Transfercode.transfer."""
        if dry_run:
            return self
        if not self.needs_transcode:
            return self
        if not (force or self.needs_update()):
            return self
        tempname = os.path.split(self.dest)[1]
        temp_basename, temp_ext = os.path.splitext(tempname)
        temp_filename = tempfile.mkstemp(prefix=temp_basename + "_", suffix=temp_ext, dir=tempdir)[1]
        Transfercode(self.src, temp_filename, self.eopts, self.use_checksum).transfer(force=True, *args, **kwargs) # type: ignore
        return TransfercodeTemp(temp_filename, self.dest, None, False)

class TransfercodeTemp(Transfercode):
    """Transfercode subclass where source is a temp file.

The only addition is that the source file is deleted after transfer."""
    def transfer(self, *args, **kwargs) -> None:
        super(TransfercodeTemp, self).transfer(*args, **kwargs)
        os.remove(self.src)

    def transcode_to_tempdir(self, *args, **kwargs) -> 'TransfercodeTemp':
        """This would be redundant."""
        return self

    def needs_update(self, *args, **kwargs) -> bool:
        return True

def is_subpath(path: str, parent: str) -> bool:
    """Returns true if path is a subpath of parent.

    For example, '/usr/bin/python' is a subpath of '/usr', while
    '/bin/ls' is not. Both paths must be absolute, but no checking is
    done. Behavior on relative paths is undefined.

    Any path is a subpath of itself."""
    # Any relative path that doesn't start with ".." is a subpath.
    return not os.path.relpath(path, parent)[0:2].startswith(os.path.pardir)

def walk_files(dir: str, hidden: bool=False) -> Iterable[str]:
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
    def __init__(self, src_dir: str, dest_dir: str, src_exts: Iterable[str],
                 dest_ext: str, hidden: bool=False) -> None:
        self.src_dir = os.path.realpath(src_dir)
        self.dest_dir = os.path.realpath(dest_dir)
        # These need leading dots
        self.src_exts = list(src_exts)
        self.dest_ext = dest_ext
        self.include_hidden = hidden

    def find_dest(self, src: str) -> str:
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

    def walk_source_files(self) -> Iterable[str]:
        """An iterator over all files in the source directory."""
        return walk_files(self.src_dir, hidden=self.include_hidden)

    def walk_target_files(self) -> Iterable[str]:
        """An iterator over all files that are to be created in the destination directory."""
        return map(self.find_dest, self.walk_source_files())

    def walk_source_target_pairs(self) -> Iterable[Tuple[str, str]]:
        """iter(zip(self.walk_source_files(), self.walk_target_files()))'.

        Only it's more efficient."""
        return ((src, self.find_dest(src)) for src in self.walk_source_files())

    def walk_existing_dest_files(self) -> Iterable[str]:
        """An iterator over all existing files in the destination directory."""
        return walk_files(self.dest_dir, hidden=self.include_hidden)

    def walk_extra_dest_files(self) -> Iterable[str]:
        """An iterator over all existing files in the destination directory that are not targets of source files.

        These are the files that transfercoder would delete if given the --delete option."""
        target_files = list(self.walk_target_files())
        return sorted(set(self.walk_existing_dest_files()).difference(self.walk_target_files()))

    def transfercodes(self, eopts=None, use_checksum=True) -> Iterable[Transfercode]:
        """Generate Transfercode objects for all src files.

        Optional arg 'eopts' is passed to the Transfercode() constructor."""
        return (Transfercode(src, dest, eopts, use_checksum) for src, dest in self.walk_source_target_pairs())
