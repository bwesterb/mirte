import os
import sys
import yaml
import copy
import os.path
import logging

from itertools import chain

from sarah.order import sort_by_successors, dual_cover, restricted_cover

from mirte.core import ModuleDefinition, DepDefinition, VSettingDefinition

FILE_SUFFIX = '.mirte'
DEFAULT_FILE = 'default.mirte'


def depsOf_of_mirteFile_instance_definition(man, insts):
        """ Returns a function that returns the dependencies of
            an instance definition by its name, where insts is a
            dictionary of instance definitions from a mirteFile """
        return lambda x: map(lambda a: a[1],
                             filter(lambda b: b[0] in \
                                [dn for dn, d in (
                                        man.modules[
                                                insts[x]['module']
                                                ].deps.iteritems()
                                        if 'module' in insts[x] else [])],
                                insts[x].items()))

def depsOf_of_mirteFile_module_definition(defs):
        """ Returns a function that returns the dependencies of a module
            definition by its name, where defs is a dictionary of module
            definitions from a mirteFile """
        return lambda x: (filter(lambda z: not z is None and z in defs,
                                 map(lambda y: y[1].get('type'),
                                     defs[x]['settings'].items()
                                        if 'settings' in defs[x] else []))) + \
                         (defs[x]['inherits'] if 'inherits' in defs[x] else [])

def module_definition_from_mirteFile_dict(man, d):
        """ Creates a ModuleDefinition instance from the dictionary <d> from
            a mirte-file for the Manager instance <man>. """
        m = ModuleDefinition()
        if not 'inherits' in d: d['inherits'] = list()
        if not 'settings' in d: d['settings'] = dict()
        if 'implementedBy' in d:
                m.implementedBy = d['implementedBy']
        m.inherits = set(d['inherits'])
        for p in d['inherits']:
                if not p in man.modules:
                        raise ValueError, "No such module %s" % p
                m.deps.update(man.modules[p].deps)
                m.vsettings.update(man.modules[p].vsettings)
                m.inherits.update(man.modules[p].inherits)
                m.run = m.run or man.modules[p].run
        if 'run' in d:
                m.run = d['run']
        if len(m.inherits) == 0:
                m.inherits = set(['module'])
        for k, v in d['settings'].iteritems():
                if not 'type' in v:
                        if not k in m.vsettings:
                                raise ValueError, \
                                        "No such existing vsetting %s" % k
                        if 'default' in v:
                                m.vsettings[k] = copy.copy(m.vsettings[k])
                                m.vsettings[k].default = v['default']
                        continue
                if v['type'] in man.modules:
                        m.deps[k] = DepDefinition(v['type'])
                elif v['type'] in man.valueTypes:
                        m.vsettings[k] = VSettingDefinition(v['type'],
                                (man.valueTypes[v['type']](v['default'])
                                        if 'default' in v else None))
                else:
                        raise ValueError, \
                                "No such module or valuetype %s" % v
        return m

def load_mirteFile(path, m, logger=None):
        """ Loads the mirte-file at <path> into the manager <m>. """
        l = logging.getLogger('load_mirteFile') if logger is None else logger
        had = set()
        for name, path, d in walk_mirteFiles(path):
                identifier = name
                if name in had:
                        identifier = path
                else:
                        had.add(name)
                l.info('loading %s' % identifier)
                _load_mirteFile(d, m)

def _load_mirteFile(d, m):
        """ Loads the dictionary from the mirteFile into <m> """
        defs = d['definitions'] if 'definitions' in d else {}
        insts = d['instances'] if 'instances' in d else {}
        # Filter out existing instances
        insts_to_skip = []
        for k in insts:
                if k in m.insts:
                        m.update_instance(k, dict(insts[k]))
                        insts_to_skip.append(k)
        for k in insts_to_skip:
                del(insts[k])
        # Sort module definitions by dependency
        it = sort_by_successors(defs.keys(), dual_cover(defs.keys(),
                restricted_cover(defs.keys(),
                                 depsOf_of_mirteFile_module_definition(defs))))
        # Add module definitions
        for k in it:
                m.add_module_definition(k,
                        module_definition_from_mirteFile_dict(m, defs[k]))
        # Sort instance declarations by dependency
        it = sort_by_successors(insts.keys(),
                dual_cover(insts.keys(), restricted_cover(insts.keys(),
                        depsOf_of_mirteFile_instance_definition(m, insts))))
        # Create instances
        for k in it:
                settings = dict(insts[k])
                del(settings['module'])
                m.create_instance(k, insts[k]['module'], settings)

def find_mirteFile(name, extra_path=None):
        """ Resolves <name> to a path.  Uses <extra_path> """
        extra_path = () if extra_path is None else extra_path
        for bp in chain(extra_path, sys.path):
                pb = os.path.join(bp, name) 
                p = pb + FILE_SUFFIX
                if os.path.exists(p):
                        return os.path.abspath(p)
                p = os.path.join(pb, DEFAULT_FILE)
                if os.path.exists(p):
                        return os.path.abspath(p)
        raise ValueError, "Couldn't find mirteFile %s" % name

def walk_mirteFiles(name):
        """ Yields (cpath, d) for all dependencies of and including the
            mirte-file <name>, where <d> are the dictionaries from
            the mirte-file at <cpath> """
        stack = [(name, find_mirteFile(name, (os.getcwd(),)))]
        loadStack = []
        had = dict()
        while stack:
                name, path = stack.pop()
                if path in had:
                        d = had[path]
                else:
                        with open(path) as f:
                                d = yaml.load(f)
                        had[path] = d
                loadStack.append((name, path, d))
                if not 'includes' in d:
                        continue
                for include in d['includes']:
                        stack.append((include, find_mirteFile(include,
                                (os.path.dirname(path),))))
        had = set()
        for name, path, d in reversed(loadStack):
                if path in had:
                        continue
                had.add(path)
                yield name, path, d
