"""Filesystem operations with sudo permissions"""

from siptools_research.utils.shell import Shell


def add_write_if_read_or_write(st_mode):
    """Add "x" to given `st_mode` if they contain "r" or "w".

    Useful with directories because most usually read/write also is followed
    with execute flag.

    :st_mode: Permissions to modify from `statinfo.st_mode`
    :returns: Modified permissions

    """

    st_mode_out = st_mode
    if (st_mode & 0o200) or (st_mode & 0o400):
        st_mode_out = st_mode_out | 0o100
    if (st_mode & 0o020) or (st_mode & 0o040):
        st_mode_out = st_mode_out | 0o010
    return st_mode_out


def set_permissions(uid, gid, permissions, path):
    """Set file owner and permissions recursively for given path.

    Note: This will `sudo chmod` and `sudo chown` so you most propably want to
    configure `/etc/sudoers.d` with correct permissions.

    :uid: User id to set
    :gid: Group id to set
    :permissions: POSIX permissions to set
    :path: The directory, whose permissions will be recursively set

    """

    # Usually file permissions are 0o660
    # According directory_permissions will be 0o770

    directory_permissions = add_write_if_read_or_write(permissions)

    commands = [
        ['sudo', 'chown', '-Rv', '%s:%s' % (uid, gid), path],
        ['find', path, '-type', 'f', '-exec',
         'sudo', 'chmod', '-v', '%.4o' % permissions, '{}', ';'],
        ['find', path, '-type', 'd', '-exec',
         'sudo', 'chmod', '-v', '%.4o' % directory_permissions, '{}', ';']]

    for command in commands:
        Shell(command).check_call()
