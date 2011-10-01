from __future__ import with_statement

from mirte.core import Module
from sarah.event import Event

import logging
import threading

class ThreadPool(Module):
        class Worker(threading.Thread):
                def __init__(self, pool, l):
                        threading.Thread.__init__(self)
                        self.l = l
                        self.pool = pool
                        self.name = 'pristine'
                def run(self):
                        self.l.debug("Hello")
                        self.pool.cond.acquire()
                        self.pool.actualFT += 1
                        while True:
                                if not self.pool.running:
                                        break
                                if not self.pool.jobs:
                                        self.pool.cond.wait()
                                        continue
                                job, name = self.pool.jobs.pop()
                                self.name = name
                                self.pool.actualFT -= 1
                                self.pool.cond.release()
                                try:
                                        ret = job()
                                except Exception, e:
                                        self.l.exception("Uncaught exception")
                                        ret = True
                                # delete job.  Otherwise job will stay alive
                                # while we wait on self.pool.cond
                                del(job)
                                self.pool.cond.acquire()
                                self.name = 'free'
                                self.pool.actualFT += 1
                                self.pool.expectedFT += 1
                                if not ret:
                                        break
                        self.pool.actualFT -= 1
                        self.pool.expectedFT -= 1
                        self.pool.workers.remove(self)
                        self.pool.cond.release()
                        self.l.debug("Bye (%s)" % self.name)

        def __init__(self, *args, **kwargs):
                super(ThreadPool, self).__init__(*args, **kwargs)
                self.running = True
                self.jobs = list()
                self.cond = threading.Condition()
                self.mcond = threading.Condition()
                self.actualFT = 0       # actual number of free threads
                self.expectedFT = 0     # expected number of free threads
                self.expectedT = 0      # expected number of threads
                self.ncreated = 0       # total number of threads created
                self.workers = set()
        
        def _remove_worker(self):
                self._queue(lambda: False, False)
                self.expectedT -= 1
        
        def _create_worker(self):
                self.ncreated += 1
                self.expectedFT += 1
                self.expectedT += 1
                n = self.ncreated
                l = logging.LoggerAdapter(self.l, {'sid': n})
                t = ThreadPool.Worker(self, l)
                self.workers.add(t)
                t.start()
        
        def start(self):
                self.main_thread = threading.Thread(target=self.run)
                self.main_thread.start()
        
        def run(self):
                self.mcond.acquire()
                while self.running:
                        self.cond.acquire()
                        gotoSleep = False
                        tc = max(self.minFree - self.expectedFT
                                        + len(self.jobs),
                                self.min - self.expectedT)
                        td = min(self.expectedFT - len(self.jobs)
                                        - self.maxFree,
                                self.expectedT - self.min)
                        if tc > 0:
                                for i in xrange(tc):
                                        self._create_worker()
                        elif td > 0:
                                for i in xrange(td):
                                        self._remove_worker()
                        else:
                                gotoSleep = True
                        self.cond.release()
                        if gotoSleep:   
                                self.mcond.wait()
                self.l.info("Waking and joining all workers")
                with self.cond:
                        self.cond.notifyAll()
                        workers = list(self.workers)
                self.mcond.release()
                for worker in workers:
                        while True:
                                worker.join(1)
                                if not worker.isAlive():
                                        break
                                self.l.warn("Still waiting on %s" % worker)
                self.l.info("   joined")
        def stop(self):
                self.running = False
                with self.mcond:
                        self.mcond.notify()
        
        def _queue(self, raw, name=None):
                if self.actualFT == 0:
                        self.l.warn("No actual free threads, yet "+
                                    "(increase threadPool.minFree)")
                self.jobs.append((raw, name))
                self.expectedFT -= 1
                self.cond.notify()
                self.mcond.notify()
        
        def execute_named(self, function, name=None, *args, **kwargs):
                def _entry():
                        function(*args, **kwargs)
                        return True
                with self.mcond:
                        with self.cond:
                                self._queue(_entry, name)
        
        def execute(self, function, *args, **kwargs):
                self.execute_named(function, None, *args, **kwargs)
        
        def join(self):
                self.main_thread.join()
