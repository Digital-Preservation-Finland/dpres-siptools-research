"""Object wrapper for shell commands"""

import os
import logging
import subprocess

LOGGER = logging.getLogger('siptools_research.shell')


class Shell(object):
    """Object wrapper for shell commands"""

    def __init__(self, command, cwd=None, env=None):
        """Setup class"""

        self.command = command
        self.cwd = cwd

        self.env = os.environ.copy()
        if env is not None:
            for key in env:
                self.env[key] = env[key]

        self._stdout = None
        self._stderr = None
        self._returncode = None

        self.valid_returncodes = [0]

    def run(self):
        """Run the command"""

        proc = subprocess.Popen(
            self.command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            cwd=self.cwd,
            env=self.env)

        (self._stdout, self._stderr) = proc.communicate()
        self._returncode = proc.returncode

        LOGGER.info(self)

        return self

    def check_call(self):
        """Run the command and raise exception if nonzero returncode"""

        if self._returncode is None:
            self.run()

        if self._returncode not in self.valid_returncodes:
            raise OSError(
                'Invalid returncode:\n%s' % self)

        return self

    @property
    def stdout(self):
        """Return cached stdout as string"""
        if self._stdout is None:
            self.check_call()
        return self._stdout

    @property
    def stderr(self):
        """Return cached stderr as string"""
        if self._stderr is None:
            self.check_call()
        return self._stderr

    @property
    def returncode(self):
        """Return cached returncode"""
        if self._returncode is None:
            self.check_call()
        return self._returncode

    def __str__(self):
        """Concatenate command output"""
        return '\n'.join([
            "COMMAND: %s " % self.command,
            "CWD: %s" % self.cwd,
            "RETURNCODE: %s " % self.returncode,
            "STDOUT:\n%s" % self.stdout,
            "STDERR:\n%s" % self.stderr])

class Validator(Shell):
    """Object wrapper for IPT validator"""

    def __init__(self, *args, **kwargs):
        """Initialize validator instance"""

        super(Validator, self).__init__(*args, **kwargs)
        self.valid_returncodes = [0, 117]

    @property
    def is_valid(self):
        """Return True if validator returned valid result"""
        return self.returncode == 0

    @property
    def outcome(self):
        """Return PREMIS compatible event outcome"""
        if self.returncode == 0:
            return 'success'
        return 'failure'
