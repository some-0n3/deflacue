#!/usr/bin/env python
"""
deflacue is a Cue Sheet parser and a wrapper for mighty SoX utility -
http://sox.sourceforge.net/.

SoX with appropriate plugins should be installed for deflacue to function.
Ubuntu users may install the following SoX packages: `sox`, `libsox-fmt-all`.


deflacue can function both as a Python module and in command line mode.
"""
import argparse
import logging
import os
import re

from io import open  # Py2 support
from copy import deepcopy
from subprocess import PIPE, Popen


VERSION = (1, 0, 0)


COMMENTS_VORBIS = (
    'TITLE',
    'VERSION',
    'ALBUM',
    'TRACKNUMBER',
    'ARTIST',
    'PERFORMER',
    'COPYRIGHT',
    'LICENSE',
    'ORGANIZATION',
    'DESCRIPTION',
    'GENRE',
    'DATE',
    'LOCATION',
    'CONTACT',
    'ISRC'
)

COMMENTS_CUE_TO_VORBIS = {
    'TRACK_NUM': 'TRACKNUMBER',
    'TITLE': 'TITLE',
    'PERFORMER': 'ARTIST',
    'ALBUM': 'ALBUM',
    'GENRE': 'GENRE',
    'DATE': 'DATE',
}

TIME_PATTERN = re.compile(r'(?P<mm>\d+):(?P<ss>\d\d):(?P<ff>\d\d)')


class DeflacueError(Exception):
    """Exception type raised by deflacue."""


class CueParser(object):
    """Simple Cue Sheet file parser."""

    def __init__(self, cue_file, encoding=None):
        self._context_global = {
            'PERFORMER': 'Unknown',
            'SONGWRITER': None,
            'ALBUM': 'Unknown',
            'GENRE': 'Unknown',
            'DATE': None,
            'FILE': None,
            'COMMENT': None,
        }
        self._context_tracks = tracks = []
        self._current_context = self._context_global
        try:
            with open(cue_file, encoding=encoding) as f:
                lines = f.readlines()
        except UnicodeDecodeError:
            raise DeflacueError('Unable to read data from .cue file. Please '
                                'use -encoding command line argument to set '
                                'correct encoding.')

        lines = [l.strip() for l in lines]
        lines = [l for l in lines if l]
        for line in lines:
            command, args = line.split(' ', 1)
            logging.debug('Command `%s`. Args: %s', command, args)
            method = getattr(self, 'cmd_%s' % command.lower(), None)
            if method is not None:
                method(args)
            else:
                logging.warning('Unknown command `%s`. Skipping ...', command)

        for current, _next in zip(tracks, tracks[1:]):
            current['POS_END_SAMPLES'] = _next['POS_START_SAMPLES']
        tracks[-1]['POS_END_SAMPLES'] = None

    def get_data_global(self):
        """Returns a dictionary with global CD data."""
        return self._context_global

    def get_data_tracks(self):
        """Returns a list of dictionaries with individual
        tracks data. Note that some of the data is borrowed from global data.

        """
        return self._context_tracks

    def _unquote(self, in_str):
        return in_str.strip(' "')

    def _timestr_to_samples(self, timestr):
        """Converts `mm:ss:ff` time string into samples integer, assuming the
        CD sampling rate of 44100Hz."""
        seconds_factor = 44100
        # 75 frames per second of audio
        frames_factor = seconds_factor // 75

        match = TIME_PATTERN.match(timestr)
        if not match:
            raise ValueError('"{}" is not a valid time string (it sould be'
                             ' "mm:ss:ff")'.format(timestr))
        parsed = {key: int(val) for key, val in match.groupdict().items()}
        seconds = parsed['mm'] * 60 + parsed['ss']
        return seconds * seconds_factor + parsed['ff'] * frames_factor

    def _in_global_context(self):
        return self._current_context == self._context_global

    def cmd_rem(self, args):
        subcommand, subargs = args.split(' ', 1)
        if subargs.startswith('"'):
            subargs = self._unquote(subargs)
        self._current_context[subcommand.upper()] = subargs

    def cmd_performer(self, args):
        unquoted = self._unquote(args)
        self._current_context['PERFORMER'] = unquoted

    def cmd_title(self, args):
        unquoted = self._unquote(args)
        if self._in_global_context():
            self._current_context['ALBUM'] = unquoted
        else:
            self._current_context['TITLE'] = unquoted

    def cmd_file(self, args):
        filename = self._unquote(args.rsplit(' ', 1)[0])
        self._current_context['FILE'] = filename

    def cmd_index(self, args):
        timestr = args.split()[1]
        self._current_context['INDEX'] = timestr
        self._current_context['POS_START_SAMPLES'] = \
            self._timestr_to_samples(timestr)

    def cmd_track(self, args):
        num, _ = args.split()
        new_track_context = deepcopy(self._context_global)
        self._context_tracks.append(new_track_context)
        self._current_context = new_track_context
        self._current_context['TRACK_NUM'] = int(num)

    def cmd_flags(self, args):
        pass


class Deflacue(object):
    """deflacue functionality is encapsulated in this class.

    Usage example:
        deflacue = Deflacue('/home/idle/cues_to_process/')
        deflacue.do()

    This will search `/home/idle/cues_to_process/` and subdirectories
    for .cue files, parse them and extract separate tracks.
    Extracted tracks are stored in Artist - Album hierarchy within
    `deflacue` directory under each source directory.

    """

    # Some lengthy shell command won't be executed on dry run.
    _dry_run = False

    def __init__(self, source, destination=None, encoding=None,
                 log_level=logging.INFO):
        """Prepares deflacue to for audio processing.

        `source` - Absolute or relative to the current directory path,
                   containing .cue file(s) or subdirectories with .cue
                   file(s) to process.

        `destination`   - Absolute or relative to the current directory path
                          to store output files in.
                          If None, output files are saved in `deflacue`
                          directory in the same directory as input file(s).

        `encoding`    -  Encoding used for .cue file(s).

        `log_level` - Defines the verbosity level of deflacue. All messages
                      produced by the application are logged with `logging`
                      module.
                      Examples: logging.INFO, logging.DEBUG.

        """
        self.source = os.path.abspath(source)
        self.target = destination
        self.encoding = encoding

        if log_level:
            self._configure_logging(log_level)

        logging.info('Source path: %s', self.source)
        if not os.path.exists(self.source):
            raise DeflacueError('Path `%s` is not found.' % self.source)

        if destination is not None:
            self.target = os.path.abspath(destination)
            os.chdir(self.source)

    def _process_command(self, command, stdout=None, supress_dry_run=False):
        """Executes shell command with subprocess.Popen.
        Returns tuple, where first element is a process return code,
        and the second is a tuple of stdout and stderr output.
        """
        logging.debug('Executing shell command: %s', command)
        if not self._dry_run or supress_dry_run:
            prc = Popen(command, shell=True, stdout=stdout)
            std = prc.communicate()
            return prc.returncode, std
        return 0, ('', '')

    def _configure_logging(self, verbosity_lvl=logging.INFO):
        """Switches on logging at given level."""
        logging.basicConfig(level=verbosity_lvl,
                            format='%(levelname)s: %(message)s')

    def _create_directory(self, path):
        """Creates a directory for target files."""
        if not os.path.exists(path) and not self._dry_run:
            logging.debug('Creating target path: %s ...', path)
            try:
                os.makedirs(path)
            except OSError:
                raise DeflacueError('Unable to create target path: %s.' % path)

    def set_dry_run(self):
        """Sets deflacue into dry run mode, when all requested actions
        are only simulated, and no changes are written to filesystem.

        """
        self._dry_run = True

    def get_cue_files(self, recursive=False):
        """Yield all directories and a list of their .cue files.
        `recursive` - if True search is also performed within subdirectories.

        """
        def is_cue(name):
            """Check if the file is a .cue file."""
            return os.path.splitext(name)[1].lower() == '.cue'

        logging.info('Enumerating cue-files under the source path '
                     '(recursive=%s) ...', recursive)
        if recursive:
            for root, _, files in os.walk(self.source):
                cue_files = sorted([f for f in files if is_cue(f)])
                if cue_files:
                    yield os.path.join(self.source, root), cue_files
        else:
            files = [f for f in os.listdir(self.source)
                     if os.path.isfile(os.path.join(self.source, f))]
            yield self.source, sorted([f for f in files if is_cue(f)])

    def sox_check_is_available(self):
        """Checks whether SoX is available."""
        result = self._process_command('sox -h', PIPE, supress_dry_run=True)
        return result[0] == 0

    def sox_extract_audio(self, source, start, end, target, metadata=None):
        """Using SoX extracts a chunk from source audio file into target."""
        logging.info('Extracting `%s` ...', os.path.basename(target))

        length = '' if end is None else "%ss" % (end - start)

        add_comment = []
        if metadata is not None:
            logging.debug('Metadata: %s\n', metadata)
            for key, val in COMMENTS_CUE_TO_VORBIS.items():
                if key in metadata and metadata[key] is not None:
                    add_comment.append('--add-comment="%s=%s"' %
                                       (val, metadata[key]))
        add_comment = ' '.join(add_comment)

        logging.debug('Extraction information:\n'
                      '      Source file: %(file)s\n'
                      '      Start position: %(start)s samples\n'
                      '      End position: %(end)s samples\n'
                      '      Length: %(length)s sample(s)',
                      {'file': source, 'start': start, 'end': end,
                       'length': length})
        command = 'sox -V1 "{source}" --comment="" {comment} "{target}" trim' \
                  ' {start}s {length}'.format(source=source, target=target,
                                              start=start, length=length,
                                              comment=add_comment)

        if not self._dry_run:
            self._process_command(command, PIPE)

    def process_cue(self, cue_file, target_path):
        """Parses .cue file, extracts separate tracks."""
        logging.info('Processing `%s`\n', os.path.basename(cue_file))
        parser = CueParser(cue_file, encoding=self.encoding)
        cd_info = parser.get_data_global()
        if not os.path.exists(cd_info['FILE']):
            logging.error('Source file `%s` is not found. Cue Sheet is '
                          'skipped.', cd_info['FILE'])
            return

        tracks = parser.get_data_tracks()

        title = cd_info['ALBUM']
        if cd_info['DATE'] is not None:
            title = '%s - %s' % (cd_info['DATE'], title)

        try:  # Py2 support
            target_path = target_path.decode('utf-8')
        except AttributeError:
            pass

        bundle_path = os.path.join(target_path, cd_info['PERFORMER'], title)
        self._create_directory(bundle_path)

        tracks_count = len(tracks)
        for track in tracks:
            track_num = str(track['TRACK_NUM']).zfill(len(str(tracks_count)))
            filename = '%s - %s.flac' % (track_num,
                                         track['TITLE'].replace('/', ''))
            self.sox_extract_audio(track['FILE'], track['POS_START_SAMPLES'],
                                   track['POS_END_SAMPLES'],
                                   os.path.join(bundle_path, filename),
                                   metadata=track)

    def do(self, recursive=False):
        """Main method processing .cue files in batch."""
        initial_cwd = os.getcwd()

        for path, cue_files in sorted(self.get_cue_files(recursive)):
            os.chdir(path)
            logging.info('\n%s\n      Working on: %s\n', '=' * 40, path)

            if self.target is None:
                # When a target path is not specified, create `deflacue`
                # subdirectory in every directory we are working at.
                target_path = os.path.join(path, 'deflacue')
            else:
                # When a target path is specified, we create a subdirectory
                # there named after the directory we are working on.
                target_path = os.path.join(self.target, os.path.basename(path))

            self._create_directory(target_path)
            logging.info('Target (output) path: %s', target_path)

            for cue_file in cue_files:
                self.process_cue(os.path.join(path, cue_file), target_path)

        os.chdir(initial_cwd)
        logging.info('We are done. Thank you.\n')


def main():

    argparser = argparse.ArgumentParser('deflacue.py')
    argparser.add_argument(
        'source_path',
        help='Absolute or relative source path with .cue file(s).'
    )
    argparser.add_argument(
        '-r', '--recursive', action='store_true',
        help='Recursion flag to search directories under the source_path.',
    )
    argparser.add_argument(
        '-d', '--destination',
        help='Absolute or relative destination path for output audio file(s).'
    )
    argparser.add_argument('-e', '--encoding',
                           help='Cue Sheet file(s) encoding.')
    argparser.add_argument(
        '--dry', action='store_true',
        help='Perform the dry run with no changes done to filesystem.',
    )
    argparser.add_argument('--debug', action='store_true',
                           help='Show debug messages while processing.')

    parsed = argparser.parse_args()
    kwargs = {
        'source': parsed.source_path,
        'encoding': parsed.encoding,
        'destination': parsed.destination,
    }
    if parsed.debug:
        kwargs['log_level'] = logging.DEBUG

    try:
        deflacue = Deflacue(**kwargs)

        if not deflacue.sox_check_is_available():
            raise DeflacueError(
                'SoX seems not available. Please install it '
                '(e.g. `sudo apt-get install sox libsox-fmt-all`).'
            )

        if parsed.dry:
            deflacue.set_dry_run()

        deflacue.do(parsed.recursive)
    except DeflacueError as e:
        logging.error(e)


if __name__ == '__main__':
    main()
