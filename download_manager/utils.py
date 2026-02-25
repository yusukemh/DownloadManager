from itertools import product
import numpy as np

class ArrayParser():
    """Given list of iterables, allows obtaining i-th combination in cartesian product.
    Useful for unpacking nested iteration when only one loop is allowd, e.g., SLURM ARRAY.
    Example:
        p = ArrayParser(year=range(1950, 1980), region=['oahu', 'maui', 'kauai', 'big_island'])
        p(0) = {'year': 1950, 'region': 'oahu'}
        p(-1) = {'year': 1979, 'region': 'big_island'}
    """
    def __init__(self, **kwargs):
        self.products = list(product(*[v for k, v in kwargs.items()]))
        self.keys = list(kwargs.keys())
        self._Ns = [len(v) for k, v in kwargs.items()]

    def unpack(self, i):
        return {k: v for k, v in zip(self.keys, self.products[i])}

    def __len__(self):
        return np.prod(self._Ns)
    
    def __call__(self, i):
        """
        """
        return self.unpack(i)

    def __iter__(self):
        """Allow use like 
        ```
        array_parser = ArrayParser(...)
        for kwarg in array_parser:
            print(kwarg)
        ```
        """
        for i in range(len(self)):
            yield self.unpack(i)

    def __getitem__(self, i):
        """Allows use like
        ```
        array_parser = ArrayParser(...)
        for kwarg in array_parser[:10]:
            print(kwarg)
        ```
        """
        if isinstance(i, slice):
            start = i.start if i.start is not None else 0
            stop = i.stop if i.stop is not None else len(self)
            step = i.step if i.step is not None else 1
            return [self.unpack(i) for i in range(start, stop, step)]
        else:
            return self.unpack(i)
    
    def __repr__(self):
        s = '<ArrayParser ['
        s += ''.join([f"{k}({n}), " for k, n in zip(self.keys, self._Ns)])[:-2]
        s += ']>'
        return s