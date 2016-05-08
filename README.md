# Transfercoder: Transfer and transcode your music

This script is my answer to the common problem of wanting to put one's
music library onto a device that doesn't support some of your music
formats. All the solutions that I've seen to this problem have at least one of the following drawbacks:

* Assume that I want to transcode *everything* into a single format,
  even though the device supports multiple formats.
* Incorrect handling of tags (title, artist, etc.) during transcoding
* Require a large media player

So I wrote my own script to do it. You specify your music directory
and the device's music directory, as well as the file extensions that
require transcoding and the format to transcode to. All your music is
copied to the device, preserving the directory structure. Any files
with the specified extensions are transcoded, keeping the same base
name and changing the extension. All other files are copied with no
modification. This includes *all* other files, not just other music
files. So your album art and stuff gets transferred too. Transcoding
happens in parallel if you have multiple cores avilable. Running the
same script a second time will only update files that have changed in
your music directory. The default transcoding options will transcode
several lossless formats to ogg.

In order to know when a source file has been updated, this script
saves the first 32 bytes sha256 hash of the source file in the
`transfercoder_src_checksum` tag in each transcoded destination file.
This means that simply changing the time stamps of files in your music
directory will not cause them all the be transcoded again. If you
don't want this, then use the `--no-checksum-tags` option, and it will
avoid adding these tags and instead rely on the file time stamps,
transcoding a file any time the source file is newer.

# Usage

Put the script in your path. Install the prereqs. Then, use as so:

    $ transfercoder /home/yourname/Music /media/musicplayer/music

See the help for more options. Or ask me.

# Prerequisites

* Python with the plac and mutagen modules installed
* ffmpeg - For transcoding (Make sure ffmpeg is complied with encode
  and/or decode support for the formats you require)
* Rsync (optional) - For faster copying of small changes

As indicated, rsync is an optional dependency. It is primarily be
useful if you changed only file tags on files that do not require
transcoding, in which case there will only be a small difference to
transfer.

# Limitations

* You can only choose one target transcoding target format. I can't
  see a reason you would want more than one. You wouldn't want to
  transcode flac to ogg and then transcode wavpack to mp3.
* No playlist support. Playlists can get complicated. They may have
  relative or absolute paths, and the device may expect them in odd
  formats. You'll need to come up with your own solution here, or just
  use a media player that handles them.
* Replaygain tags are intentionally not copied. This is because in my
  experience, different formats of the same song need different
  adjustments, even though in theory they should have identical
  volumes. So just add replaygain to your transcoded library on your
  device after syncing. If only there was a
  [tool](https://github.com/DarwinAwardWinner/rganalysis) for that
  too.
