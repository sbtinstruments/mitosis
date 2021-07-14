from pydantic import BaseModel

class SpecificPort(BaseModel):
    node: str
    port: str

    def __hash__(self):
        """Workaround to use this as dict keys"""
        return hash((type(self),) + (self.node, self.port))

class EdgeModel(BaseModel):
    """An edge between two ports"""
    start: SpecificPort
    end: SpecificPort
    active: bool = True

    def __hash__(self):
        """Workaround to use this as dict keys"""
        return hash((type(self),) + (self.start, self.end))