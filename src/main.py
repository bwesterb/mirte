import optparse
import logging
import os.path
import sys

from mirte.core import Manager
from mirte.mirteFile import load_mirteFile

from sarah.order import sort_by_successors
import sarah.coloredLogging

def parse_cmdLine(args):
	""" Parses commandline arguments into options and arguments """
	options = dict()
	rargs = list()
	for arg in args:
		if arg[:2] == '--':
			tmp = arg[2:]
			bits = tmp.split('=', 1)
			if len(bits) == 1:
				bits.append('')
			options[bits[0]] = bits[1]
		else:
			rargs.append(arg)
	return options, rargs

def execute_cmdLine_options(options, m, l):
	""" Applies the instructions given via <options> on the manager <m> """
	opt_lut = dict()
	inst_lut = dict()
	for k, v in options.iteritems():
		bits = k.split('-', 1)
		if len(bits) == 1:
			inst_lut[bits[0]] = v
		else:
			if not bits[0] in opt_lut:
				opt_lut[bits[0]] = list()
			opt_lut[bits[0]].append((bits[1], v))
	inst_list = sort_by_successors(inst_lut.keys(),
			lambda inst: [v for (k,v) in opt_lut.get(inst, ())
					if k in m.modules[inst_lut[inst]].deps])
	for k in reversed(tuple(inst_list)):
		if k in m.insts:
			raise NotImplementedError, \
				"Overwriting instancens not yet supported"
		settings = dict()
		if k in opt_lut:
			for k2, v2 in opt_lut[k]:
				settings[k2] = v2
		m.create_instance(k, inst_lut[k], settings)
	for k in opt_lut:
		if k in inst_lut:
			continue
		for k2, v2 in opt_lut[k]:
			if not k in m.insts:
				raise ValueError, "No such instance %s" % k
			m.change_setting(k, k2, v2)

def main():
	""" Entry-point """
	sarah.coloredLogging.basicConfig(level=logging.DEBUG,
	    format="%(relativeCreated)d %(levelname)-8s%(name)s:%(message)s")
	l = logging.getLogger('mirte')
	options, args = parse_cmdLine(sys.argv[1:])
	m = Manager(l)
	path = args[0] if len(args) > 0 else 'default'
	load_mirteFile(path, m, logger=l)
	execute_cmdLine_options(options, m, l)
	m.run()

if __name__ == '__main__':
	if os.path.abspath('.') in sys.path:
		sys.path.remove(os.path.abspath('.'))
	main()
