from .search_types import *


class BaseSearchSpace:

    def __init__(self, name: str, base_type: BaseType, default):
        self.name = name
        self.db_name = "param_" + name
        self.base_type = base_type
        self.default = default

    def sample(self) -> list:
        pass


class GridSearchSpace(BaseSearchSpace):

    def __init__(self, name: str, default, domain: list):
        base_type = TypeMap[type(default)]
        super().__init__(name, base_type, default)
        self.domain = domain
        for x in domain:
            try:
                base_type.python_type(x)
            except ValueError:
                raise ValueError("type doesn't match: cannot convert " + str(x) + 
                                 " to " + str(base_type.python_type))

    def sample(self) -> list:
        return self.domain
