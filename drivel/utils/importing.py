def import_preferential(name, *morenames):
    if not isinstance(name, (list, tuple)):
        name = [name]
    names = list(name) + list(morenames)
    namestr = ', '.join(names[:])
    while names:
        name = names.pop(0)
        try:
            return __import__(name, {}, {}, [''])
        except ImportError:
            pass
    raise ImportError('Could not import anything from %s' % (namestr, ))
