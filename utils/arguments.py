DEFAULT = -1


def update(args, addition=None):
    if addition:
        args = list(args) + (addition if isinstance(addition, (list, tuple)) else [addition])
    if len(args) == 1 and isinstance(args[0], (list, tuple)):
        args = args[0]
    return args


def undefault(current, default):
    if current == DEFAULT:
        return default
    else:
        return current
