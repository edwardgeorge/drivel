def import_preferential(name, *morenames):
    if not isinstance(name, (list, tuple)):
        name = [name]
    names = list(name) + list(morenames)
    while names:
        name = names.pop(0)
        try:
            return __import__(name, {}, {}, [''])
        except ImportError:
            pass
