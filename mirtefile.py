import os
import yaml
import os.path

def depsOf_of_mirteFile_instance_definition(man, insts):
	""" Returns a function that returns the dependencies of
	    an instance definition by its name, where insts is a
	    dictionary of instance definitions from a mirteFile """
	return lambda x: map(lambda a: a[1],
			     filter(lambda b: b[0] in \
				[dn for dn, d in man.modules[insts[x][
					'module']].deps.iteritems()],
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
	if 'run' in d and d['run']:
		m.run = True
	m.inherits = set(d['inherits'])
	for p in d['inherits']:
		if not p in man.modules:
			raise ValueError, "No such module %s" % p
		m.deps.update(man.modules[p].deps)
		m.vsettings.update(man.modules[p].vsettings)
		m.inherits.update(man.modules[p].inherits)
		m.run = m.run or man.modules[p].run
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
	for path, d in walk_mirteFiles(path):
		l.info('loading %s' % path)
		_load_mirteFile(d, m)

def _load_mirteFile(d, m):
	""" Loads the dictionary from the mirteFile into <m> """
	defs = d['definitions'] if 'definitions' in d else {}
	insts = d['instances'] if 'instances' in d else {}
	it = sort_by_successors(defs.keys(), dual_cover(defs.keys(),
		restricted_cover(defs.keys(),
				 depsOf_of_mirteFile_module_definition(defs))))
	for k in it:
		m.add_module_definition(k,
			module_definition_from_mirteFile_dict(m, defs[k]))
	it = sort_by_successors(insts.keys(),
		dual_cover(insts.keys(), restricted_cover(insts.keys(),
			depsOf_of_mirteFile_instance_definition(m, insts))))
	for k in it:
		settings = dict(insts[k])
		del(settings['module'])
		m.create_instance(k, insts[k]['module'], settings)

def walk_mirteFiles(path):
	""" Yields (cpath, d) for all dependencies of and including the
	    mirte-file at <path>, where <d> are the dictionaries from
	    the mirte-file at <cpath> """
	stack = [path]
	loadStack = []
	had = dict()
	while stack:
		path = stack.pop()
		if os.path.abspath(path) in had:
			d = had[os.path.abspath(path)]
		else:
			with open(path) as f:
				d = yaml.load(f)
			had[os.path.abspath(path)] = d
		loadStack.append((path, d))
		if not 'includes' in d:
			continue
		for include in d['includes']:
			p = os.path.join(os.path.dirname(path),
						  include)
			stack.append(p)
	had = set()
	for path, d in reversed(loadStack):
		if os.path.abspath(path) in had:
			continue
		had.add(os.path.abspath(path))
		yield path, d
