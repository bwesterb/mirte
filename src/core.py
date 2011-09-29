import threading
import logging
import sys
import os

from itertools import product, chain

from sarah.event import Event
from sarah.order import sort_by_successors
from sarah.runtime import get_by_path
from sarah._itertools import pick
from sarah._threading import KeyboardInterruptableEvent

class Module(object):
        def __init__(self, settings, logger):
                for k, v in settings.items():
                        setattr(self, k, v)
                self.l = logger
                self.on_settings_changed = dict()
        
        def change_setting(self, key, value):
                setattr(self, key, value)
                if not key in self.on_settings_changed:
                        return
                self.on_settings_changed[key]()

        def register_on_setting_changed(self, key, handler):
                if not key in self.on_settings_changed:
                        self.on_settings_changed[key] = Event()
                self.on_settings_changed[key].register(handler)


class Manager(Module):
        def __init__(self, logger=None):
                if logger is None:
                        logger = logging.getLogger(object.__repr__(self))
                super(Manager, self).__init__({}, logger)
                self.running = False
                self.running_event = threading.Event()
                self.modules = dict()
                self.to_stop = list() # objects to stop
                self.daemons = list() # and to join
                self.valueTypes = {'str': str,
                                   'float': float,
                                   'int': int}
                self.insts = dict()
                # module -> concrete modules implementing module
                self.modules_implementing = dict()
                self.insts_implementing = dict()
                self.add_module_definition('module', ModuleDefinition())
                self.add_module_definition('manager', ModuleDefinition())
                self.add_module_definition('threadPool', ModuleDefinition(
                        vsettings={'minFree': VSettingDefinition('int', 4),
                                   'maxFree': VSettingDefinition('int', 8),
                                   'min': VSettingDefinition('int', 8)},
                        implementedBy='mirte.threadPool.ThreadPool'))
                self.register_instance('manager', 'manager', self, {}, {})
                self.create_instance('threadPool', 'threadPool', {})
                self.sleep_event = KeyboardInterruptableEvent()

        def _get_all(self, _type):
                """ Gets all instances implementing type <_type> """
                if not _type in self.modules:
                        raise ValueError, "No such module, %s" % _type
                if not self.insts_implementing.get(_type, None):
                        raise ValueError, "No instance implementing %s" % _type
                return self.insts_implementing[_type]
        
        def get_a(self, _type):
                """ Gets an instance implementing type <_type> """
                return self.insts[self._get_a(_type)].object
        def _get_a(self, _type):
                """ Gets an instance implementing type <_type> """
                tmp = self._get_all(_type)
                ret = pick(tmp)
                if len(tmp) != 1:
                        self.l.warn(("get_a: %s all implement %s; "+
                                     "picking %s") % (tmp, _type, ret))
                return ret

        def got_a(self, _type):
                """ Returns whether there is an instance implementing <_type>
                """
                if not _type in self.modules:
                        raise ValueError, "No such module, %s" % _type
                return (_type in self.insts_implementing and
                        self.insts_implementing[_type])

        class GoCa_Plan(object):
                """ A partial plan for a get_or_create_a call """
                def __init__(self, man, targets, insts=None,
                                insts_implementing=None):
                        self.man = man
                        self.targets = targets
                        self.insts = dict() if insts is None else insts
                        self.insts_implementing = (dict() if insts_implementing
                                        is None else insts_implementing)
                def free_instance_name_like(self, name):
                        if (not name in self.insts and
                                        not name in self.man.insts):
                                return name
                        suffix = 2
                        while True:
                                shot = "%s-%s" % (name, suffix)
                                if (not shot in self.insts and
                                                not name in self.man.insts):
                                        return shot
                                suffix += 1
                def got_a(self, _type):
                        if self.man.got_a(_type):
                                return True
                        return (_type in self.insts_implementing and
                                        self.insts_implementing[_type])
                def get_all(self, _type):
                        ret = list()
                        if self.man.got_a(_type):
                                ret.extend(self.man._get_all(_type))
                        if _type in self.insts_implementing:
                                ret.extend(self.insts_implementing[_type])
                        return ret
                def get_a(self, _type):
                        return pick(self.get_all(_type))
                @property
                def finished(self):
                        return not self.targets
                def plan_a(self, mod):
                        name = self.free_instance_name_like(mod)
                        self.insts[name] = (name, mod, {})
                        md = self.man.modules[mod]
                        for mod2 in chain(md.inherits, (mod,)):
                                if not mod2 in self.insts_implementing:
                                        self.insts_implementing[mod2] = list()
                                self.insts_implementing[mod2].append(name)
                        for depName, dep in md.deps.iteritems():
                                if dep.type in self.targets:
                                        self.targets[dep.type].append(
                                                        (name, depName))
                                else:
                                        self.targets[dep.type] = [
                                                        (name,depName)]
                        return name
                def branches(self):
                        choices = dict()
                        for target in self.targets:
                                if self.got_a(target):
                                        choices[target] = [(True, name) for
                                                name in self.get_all(target)]
                                        continue
                                choices[target] = [(False, name) for name
                                        in self.man.modules_implementing[
                                                target]]
                        choices_t = choices.items()
                        for choice in product(*[xrange(len(v))
                                        for k, v in choices_t]):
                                plan2 = Manager.GoCa_Plan(self.man, dict(),
                                                dict(self.insts),
                                                dict(self.insts_implementing))
                                tmp = [(choices_t[n][0], choices_t[n][1][m])
                                                for n, m in enumerate(choice)]
                                for target, inst_or_mod in tmp:
                                        if inst_or_mod[0]:
                                                name = inst_or_mod[1]
                                        else:
                                                name = plan2.plan_a(
                                                                inst_or_mod[1])
                                        for _in, depName in \
                                                        self.targets[target]:
                                                plan2.insts[_in][2][
                                                                depName] = name
                                yield plan2
                def execute(self):
                        insts = frozenset(self.insts.keys())
                        inst_list = tuple(sort_by_successors(insts,
                                lambda inst: [self.insts[inst][2][k] for k
                                in self.man.modules[self.insts[inst][1]].deps
                                if self.insts[inst][2][k] in insts]))
                        for name in reversed(inst_list):
                                self.man.create_instance(*self.insts[name])

        def get_or_create_a(self, _type):
                """ Gets or creates an instance of type <_type> """
                return self.insts[self._get_or_create_a(_type)].object

        def _get_or_create_a(self, _type):
                """ Gets or creates an instance of type <_type> """
                self.l.debug("get_or_create_a: %s" % _type)
                stack = [Manager.GoCa_Plan(self, {_type:()})]
                while stack:
                        p = stack.pop()
                        if p.finished:
                                p.execute()
                                return p.get_a(_type)
                        for c in p.branches():
                                stack.append(c)
                raise NotImplementedError
        
        def free_instance_name_like(self, name):
                if not name in self.insts: return name
                suffix = 2
                while True:
                        shot = "%s-%s" % (name, suffix)
                        if not shot in self.insts:
                                return shot
                        suffix += 1

        def add_module_definition(self, name, definition):
                if name in self.modules:
                        raise ValueError, "Duplicate module name"
                self.modules[name] = definition
                if not definition.implementedBy is None:
                        for t in chain(definition.inherits, (name,)):
                                if not t in self.modules_implementing:
                                        self.insts_implementing[t] = set()
                                        self.modules_implementing[t] = set()
                                self.modules_implementing[t].add(name)
        
        def update_instance(self, name, settings):
                """ Updates settings of instance <name> with the
                    dictionary <settings>. """
                if not name in self.insts:
                        raise ValueError, \
                                "There's no instance named %s" % name
                if 'module' in settings:
                        raise ValueError, \
                                ("Can't change module of existing instan"
                                        +"ce %s") % name
                self.l.info('update instance %-15s' % (name))
                for k, v in settings.iteritems():
                        self.change_setting(name, k, v)

        def create_instance(self, name, moduleName, settings):
                """ Creates an instance of <moduleName> at <name> with
                    <settings>. """
                if name in self.insts:
                        raise ValueError, \
                                "There's already an instance named %s" % \
                                                name
                if not moduleName in self.modules:
                        raise ValueError, \
                                "There's no module %s" % moduleName
                md = self.modules[moduleName]
                deps = dict()
                for k, v in md.deps.iteritems():
                        if not k in settings:
                                settings[k] = self._get_or_create_a(
                                                v.type)
                        if not settings[k] in self.insts:
                                raise ValueError, "No such instance %s" \
                                                % settings[k]
                        deps[k] = settings[k]
                        settings[k] = self.insts[settings[k]].object
                for k, v in md.vsettings.iteritems():
                        if not k in settings:
                                settings[k] = v.default
                                if v.default is None:
                                        self.l.warn('%s:%s not set' % 
                                                        (name, k))
                self.l.info('create_instance %-15s %s' % (
                                name, md.implementedBy))
                cl = get_by_path(md.implementedBy)
                il = logging.getLogger(name)
                obj = cl(settings, il)
                self.register_instance(name, moduleName, obj, settings, deps)
        
        def register_instance(self, name, moduleName, obj, settings, deps):
                md = self.modules[moduleName]
                self.insts[name] = InstanceInfo(name, moduleName, obj,
                                                settings, deps)
                for mn in chain(md.inherits, (moduleName,)):
                        if not mn in self.insts_implementing:
                                self.insts_implementing[mn] = set()
                        self.insts_implementing[mn].add(name)
                if md.run:
                        self.to_stop.append(name)
                        self.daemons.append(name)
                elif hasattr(obj, 'stop'):
                        self.to_stop.append(name)
        
        def run(self):
                def _daemon_entry(ii):
                        try:
                                ii.object.run()
                        except Exception:
                                self.l.exception(("Module %s exited "+
                                                  "abnormally") % ii.name)
                                return
                        self.l.info("Module %s exited normally" % ii.name)
                assert not self.running
                self.running = True
                self.running_event.set()
                tp = self.insts['threadPool'].object
                tp.start()
                # Note that self.daemons is already dependency ordered for us
                for name in self.daemons:
                        ii = self.insts[name]
                        tp.execute_named(_daemon_entry,
                                        "mirte run %s" % name, ii)
                while self.running:
                        try:
                                self.sleep_event.wait()
                        except KeyboardInterrupt:
                                self.l.warn("Keyboard interrupt")
                                self.stop()
                        self.l.info("Woke up")
                self.l.info("Stopping modules")
                for name in reversed(self.to_stop):
                        ii = self.insts[name]
                        self.l.info("  %s" % ii.name)
                        ii.object.stop()
                self.l.info("Joining threadPool")
                tp.join()
        
        def change_setting(self, instance_name, key, raw_value):
                """ Change the settings <key> to <raw_value> of an instance
                    named <instance_name>.  <raw_value> should be a string and
                    is properly converted. """
                ii = self.insts[instance_name]
                mo = self.modules[ii.module]
                if key in mo.deps:
                        if not raw_value in self.insts:
                                raise ValueError, "No such instance %s" % \
                                                raw_value
                        vii = self.insts[raw_value]
                        vmo = self.modules[vii.module]
                        if not (mo.deps[key].type in vmo.inherits or
                                        mo.deps[key].type == vii.module):
                                raise ValueError, "%s isn't a %s" % (
                                                raw_value, mo.deps[key].type)
                        value = vii.object
                elif key in mo.vsettings:
                        value = self.valueTypes[mo.vsettings[key].type](
                                        raw_value)
                else:
                        raise ValueError, "No such settings %s" % key
                self.l.info("Changing %s.%s to %s" % (instance_name,
                                                      key,
                                                      raw_value))
                ii.settings[key] = value
                ii.object.change_setting(key, value)

        def stop(self):
                if not self.running:
                        return
                self.running_event.clear()
                self.running = False
                self.sleep_event.set()

class InstanceInfo(object):
        def __init__(self, name, module, obj, settings, deps):
                self.deps = deps
                self.settings = settings
                self.name = name
                self.object = obj
                self.module = module

class VSettingDefinition(object):
        def __init__(self, _type=None, default=None):
                self.default = default
                self.type = _type

class DepDefinition(object):
        def __init__(self, _type=None):
                self.type = _type

class ModuleDefinition(object):
        def __init__(self, deps=None, vsettings=None, implementedBy=None,
                        run=False, inherits=None):
                self.deps = dict() if deps is None else deps
                self.vsettings = dict() if vsettings is None else vsettings
                self.implementedBy = implementedBy
                self.run = run
                self.inherits = list() if inherits is None else inherits
        
        @property
        def abstract(self):
                return self.implementedBy is None
