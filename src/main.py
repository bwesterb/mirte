import logging
import os.path
import sys

from mirte.core import Manager
from mirte.mirteFile import load_mirteFile

from sarah.order import sort_by_successors
import sarah.coloredLogging

def parse_cmdLine_instructions(args):
    """ Parses command-line arguments.  These are
        instruction to the manager to create instances and
        put settings. """
    instructions = dict()
    rargs = list()
    for arg in args:
        if arg[:2] == '--':
            tmp = arg[2:]
            bits = tmp.split('=', 1)
            if len(bits) == 1:
                bits.append('')
            instructions[bits[0]] = bits[1]
        else:
            rargs.append(arg)
    return instructions, rargs

def execute_cmdLine_instructions(instructions, m, l):
    """ Applies the instructions given via
        <instructions> on the manager <m> """
    opt_lut = dict()
    inst_lut = dict()
    for k, v in instructions.iteritems():
        bits = k.split('-', 1)
        if len(bits) == 1:
            if not v in m.modules:
                raise KeyError, "No such module: %s" % v
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

class MirteFormatter(logging.Formatter):
    def __init__(self):
        pass
    def format(self, record):
        record.message = record.getMessage()
        if 'sid' in record.__dict__:
            record.name += '.'+str(record.sid)
        ret = ("%(relativeCreated)d %(levelname)"+
            "-8s%(name)s:%(message)s") % record.__dict__
        if record.exc_info:
            ret += self.formatException(record.exc_info)
        return ret

def main():
    """ Entry-point """
    sarah.coloredLogging.basicConfig(level=logging.DEBUG,
                formatter=MirteFormatter())
    l = logging.getLogger('mirte')
    instructions, args = parse_cmdLine_instructions(sys.argv[1:])
    m = Manager(l)
    load_mirteFile(args[0] if args else 'default', m, logger=l)
    execute_cmdLine_instructions(instructions, m, l)
    m.run()


if __name__ == '__main__':
    if os.path.abspath('.') in sys.path:
        sys.path.remove(os.path.abspath('.'))
    main()
# vim: et:sta:bs=2:sw=4:
