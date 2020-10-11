import subprocess
import re

CHIO_CMD = 'chio'
name_pattern = r'(?P<name>[a-zA-Z]+\s[0-9]+):(\s+<(?P<status>\S+)>)?'
voltag_pattern = r'.*?(\svoltag:\s<(?P<voltag>\S+):\S*?>)'  # explicit \s required to not match avoltag
source_pattern = r'(source:\s<(?P<source>(?!>).+?)>)'


def parse_chio_status_line(line):
    try:
        result_dict = re.search(name_pattern, line).groupdict()
        status = result_dict.pop('status')
        if status:
            result_dict['status'] = status.split(',')
    except AttributeError:
        raise ValueError(f'Line "{line}" is not a valid chio status line.')
    try:
        result_dict.update(re.search(voltag_pattern, line).groupdict())
    except AttributeError:
        pass
    try:
        result_dict.update(re.search(source_pattern, line).groupdict())
    except AttributeError:
        pass
    return result_dict


def parse_chio_status(lines):
    return {r.pop('name'): r for r in [parse_chio_status_line(line) for line in lines if line]}


def chio_options(device=None):
    options = [CHIO_CMD]
    if device:
        options += ['-f', device]
    return options


def status(device=None):
    status_options = ['-a']
    result = subprocess.run(chio_options(device) + ['status'] + status_options, capture_output=True)
    result.check_returncode()
    return parse_chio_status(result.stdout.decode('utf-8').split('\n'))


def load(volume, device=None, drive_index=0):
    result = subprocess.run(chio_options(device) + ['move', 'voltag', volume, 'drive', str(drive_index)],
                            capture_output=True)
    result.check_returncode()


def unload(device=None, drive_index=0):
    result = subprocess.run(chio_options(device) + ['return', 'drive', str(drive_index)], capture_output=True)
    result.check_returncode()


if __name__ == '__main__':
    print(status())
