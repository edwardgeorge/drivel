def dotted_import(name):
    mod, attr = name.split('.'), []
    obj = None
    while mod:
        try:
            obj = __import__('.'.join(mod), {}, {}, [''])
        except ImportError, e:
            attr.insert(0, mod.pop())
        else:
            for a in attr:
                try:
                    obj = getattr(obj, a)
                except AttributeError, e:
                    raise AttributeError('could not get attribute %s from %s'
                        ' -> %s (%r)' % (a, '.'.join(mod), '.'.join(attr),
                        obj))
            return obj
    raise ImportError('could not import %s' % name)


def import_preferential(name, *morenames):
    if not isinstance(name, (list, tuple)):
        name = [name]
    names = list(name) + list(morenames)
    while names:
        name = names.pop(0)
        try:
            return __import__(name, {}, {}, [''])
        except ImportError, e:
            pass