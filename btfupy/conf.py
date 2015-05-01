import os


def load(fp):
    """Deserialize our domain-specific configuration file."""
    obj = {}
    for line in fp:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        key, value = map(str.strip, line.split(' ', 1))
        try:
            value = int(value)
        except ValueError:
            pass
        obj[key] = value
    return obj


def dump(fp, obj):
    """Serialize our domain-specific configuration file."""
    for key, value in obj.items():
        fp.write(' '.join(map(str, [key, value])) + '\n')
