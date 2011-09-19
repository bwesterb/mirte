__names__ = ['get_a_manager']

import logging

import threading

from mirte.core import Manager

__singleton_manager = None

def get_a_manager():
        """ On first call, creates and returns a @mirte.core.Manager.  On
            subsequent calls, returns the previously created instance """
        global __singleton_manager
        if __singleton_manager is None:
                def _thread_entry():
                        m.run()
                        l.info('manager.run() returned')
                l = logging.getLogger('mirte.get_a_manager')
                l.info("Creating new instance")
                m = Manager(logging.getLogger('mirte'))
                threading.Thread(target=_thread_entry).start()
                m.running_event.wait()
                __singleton_manager = m
        return __singleton_manager
