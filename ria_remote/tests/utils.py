import os
import inspect
from functools import wraps
from nose import SkipTest
from nose.plugins.attrib import attr


def check_not_generatorfunction(func):
    """Internal helper to verify that we are not decorating generator tests"""
    if inspect.isgeneratorfunction(func):
        raise RuntimeError("{}: must not be decorated, is a generator test"
                           .format(func.__name__))


def skip_ssh(func):
    """Skips SSH-based tests if environment variable RIA_TESTS_SSH was not set
    """

    check_not_generatorfunction(func)

    @wraps(func)
    @attr('skip_ssh')
    def newfunc(*args, **kwargs):
        if 'RIA_TESTS_SSH' not in os.environ:
            raise SkipTest("Disabled, set RIA_TEST_SSH to run")
        return func(*args, **kwargs)
    return newfunc


