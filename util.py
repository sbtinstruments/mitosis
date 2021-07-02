from typing import Union, List, Set

def without_keys(d: dict, keys: Union[List[str], Set[str]]):
    return {x: d[x] for x in d if x not in keys}