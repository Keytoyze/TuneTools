from .search_types import *


class BaseSearchSpace:

    def __init__(self, name: str, base_type: BaseType, default, ignore: bool=False):
        self.name = name
        self.db_name = "param_" + name
        self.base_type = base_type
        self.default = default
        self.ignore = ignore

    def check_type(self, value_to_check):
        try:
            self.base_type.python_type(value_to_check)
            return True
        except (ValueError, TypeError):
            return False

    def sample(self) -> list:
        pass


class GridSearchSpace(BaseSearchSpace):

    def __init__(self, name: str, default, domain: list, ignore: bool=False):
        base_type = TypeMap[type(default)]
        super().__init__(name, base_type, default, ignore)
        self.domain = domain
        for x in domain:
            if not self.check_type(x):
                raise ValueError("type doesn't match: cannot convert " + str(x) + 
                                 " to " + str(base_type.python_type))

    def sample(self) -> list:
        return self.domain


class Parameter:
    def __init__(self, default, domain: list):
        self.default, self.domain = default, domain