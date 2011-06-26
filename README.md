# Transfercoder: Transfer and transcode your music

This script is my answer to the common problem of wanting to put one's
music library onto a device that doesn't support some of your music
formats. All the solutions that I've seen to this problem have at least one of the following drawbacks:

* Assume that I want to transcode *everything* into a single format,
  even though the device supports multiple formats.
* Incorrect handling of tags
* Require a large media player

So I wrote my own script to do it. You specify your music directory
and the device's music directory, as well as the file extensions that
require transcoding and the format to transcode to. All your music is
copied to the device, preserving the directory structure. Any files
with the specified extensions are transcoded, keeping the same base
name and changing the extension. All other files are copied with no
modification. This includes *all* other files, not just other music
files. So your album art and stuff gets transferred too. Running the
same script a second time will only update files that are newer in
your music directory.

The default transcoding options will transcode several lossless
formats to ogg.

PAC is in charge of transcoding, so to adjust quality settings you
must edit PAC's configuration.


# Prerequisites

* Python with plac module
* Quod Libet - For copying audio tags
* Perl Audio Converter (yes, *Perl*) - For transcoding
* Audio encoders and/or decoders for the formats that you are
  transcoding
* Rsync (optional) - For faster copying of small changes

The prerequisites are a little odd. This Python script calls a Perl
script for transcoding. Why? Because it took about five lines of code
to implement it. I would rather do it via Gstreamer, but I haven't
used it before and it looks like it would be a lot more than five
lines of code. PAC encapsulates the logic for choosing the proper
audio decoders and encoders, and Quod Libet's MusicFile class does the
same for loading and saving tags. Note that you don't need to actually
*use* Quod Libet the music player. The script simply requires one of
its modules.

As indicated, rsync is an optional dependency. It is primarily be
useful if you changed only file tags on files that do not require
transcoding, in which case there will only be a small difference to
transfer.

# Limitations

* You can only choose one target transcoding target format. I can't
  see a reason you would want more than one. You wouldn't want to
  transcode flac to ogg and then transcode wavpack to mp3.
* No parallel operation. I considered adding this, but transfers are
  disk-bound, and transcodes are cpu-bound, so it's not clear that
  there would be any advantage, not is it clear how to parrallelize
  things. It might just cause more fragmentation and seeking. One
  solution would be to transcode to temporary files in parallel and
  then put those temp files on the transfer queue along with the ones
  that don't need transcoding. But this adds a good deal of
  complexity.
* No playlist support. Playlists can get complicated. They may have
  relative or absolute paths, and the device may expect them in odd
  formats. You'll need to come up with your own solution here, or just
  use a media player that handles them.
