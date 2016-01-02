__names__ = ['get_a_manager']

import logging

import threading

from mirte.core import Manager

try:
    import prctl
except ImportError:
    prctl = None

__singleton_manager = None

def get_a_manager(threadPool_settings=None):
    """ On first call, creates and returns a @mirte.core.Manager.  On
        subsequent calls, returns the previously created instance.

        If it is the first call, it will initialize the threadPool
        with @threadPool_settings. """
    global __singleton_manager
    if __singleton_manager is None:
        def _thread_entry():
            if prctl:
                prctl.set_name('mirte manager')
            m.run()
            l.info('manager.run() returned')
        l = logging.getLogger('mirte.get_a_manager')
        l.info("Creating new instance")
        m = Manager(logging.getLogger('mirte'))
        if threadPool_settings:
            m.update_instance('threadPool', threadPool_settings)
        threading.Thread(target=_thread_entry).start()
        m.running_event.wait()
        __singleton_manager = m
    return __singleton_manager
# vim: et:sta:bs=2:sw=4:
