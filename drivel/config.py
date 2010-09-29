from __future__ import with_statement
import ConfigParser

from drivel.utils.importing import dotted_import


class Config(dict):
    """Better Config Container.

    subclass of dict.
    takes a RawConfigParser instance to constructor too.

    """
    def __init__(self, config):
        if isinstance(config, ConfigParser.RawConfigParser):
            for section in config.sections():
                self[section] = Config(dict(config.items(section)))
        else:
            for key, val in config.iteritems():
                self[key] = Config(val) if isinstance(val, dict) else val

    def import_(self, key):
        return dotted_import(self[key])

    def get(self, key, default=None):
        if isinstance(key, tuple):
            try:
                return self[key]
            except KeyError, e:
                return default
        else:
            return super(Config, self).get(key, default)

    def section_with_overrides(self, key):
        """Takes a key like base:section

        and the value of base updated with the values
        from the full key.

        """
        if ':' not in key:
            return self.get(key)
        default, _, _ = key.partition(':')
        result = Config({})
        result.update(self.get(default, Config({})))
        result.update(self.get(key, Config({})))
        return result

    def getint(self, key, default=None, return_none=False):
        val = self.get(key, default)
        if val is None and return_none:
            return val
        return int(val)

    def getfloat(self, key, default=None, return_none=False):
        val = self.get(key, default)
        if val is None and return_none:
            return val
        return float(val)

    def getboolean(self, key, default=None, return_none=True):
        val = self.get(key, default)
        if val is None and return_none:
            return None
        if isinstance(val, basestring):
            val = val.lower()
            if val in ['1', 'yes', 'true', 'on']:
                return True
            elif val in ['0', 'no', 'false', 'off']:
                return False
        elif isinstance(val, bool):
            return val
        else:
            raise ValueError()

    def sections(self):
        return self.keys()

    def sections_in_ns(self, ns):
        for key in self.iterkeys():
            _ns, _, name = key.partition(':')
            if _ns == ns and name:
                yield name, self.get(key)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError, e:
            return self.__getattribute__(key)

    def __getitem__(self, key):
        if isinstance(key, tuple):
            val = self
            for k in key:
                val = val[k]
            return val
        else:
            return dict.__getitem__(self, key)

    def __contains__(self, key):
        if isinstance(key, tuple):
            val = self
            for k in key:
                if k not in val:
                    return False
                val = val[k]
            return True
        else:
            return dict.__contains__(self, key)


def fromfile(file):
    config = ConfigParser.RawConfigParser()
    if isinstance(file, basestring):
        with open(file) as f:
            config.readfp(f)
    else:
        config.readfp(file)
    return Config(config)

# END
