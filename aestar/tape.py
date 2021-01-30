from . import chio

class AutoChanger:
    def __init__(self):
        pass

class TapeDrive:
    def __init__(self):
        pass


def get_import_volumes(chio_status, exclude_prefix='CLN'):
    volume_dicts = []
    for slot, info in chio_status.items():
        voltag = info.get('voltag')
        if voltag:
            if not 'FULL' in info['status']:
                raise Exception(f'Found volume "{voltag}" in {slot}, but slot is not marked as FULL')
            if 'ACCESS' in info['status']:
                if not voltag.startswith(exclude_prefix):
                    volume_dicts.append({'voltag': voltag})
    return volume_dicts
