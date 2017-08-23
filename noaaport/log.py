import logging


# Completely reset default logging config
ROOT = logging.getLogger()
map(ROOT.removeHandler, list(ROOT.handlers))
map(ROOT.removeFilter, list(ROOT.handlers))


fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%SZ')
stderr = logging.StreamHandler()
stderr.setLevel(logging.WARNING)
stderr.setFormatter(fmt)


def get_logger(name):
    log = logging.getLogger(name)
    log.addHandler(stderr)
    log.setLevel(logging.DEBUG)
    return log


def set_level(level_name):
    stderr.setLevel(logging.getLevelName(level_name))
