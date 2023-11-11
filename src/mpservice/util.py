from deprecation import deprecated


@deprecated(deprecated_in='0.14.1', removed_in='0.15.0')
def full_class_name(cls):
    if not isinstance(cls, type):
        cls = cls.__class__
    mod = cls.__module__
    if mod is None or mod == 'builtins':
        return cls.__name__
    return mod + '.' + cls.__name__
