class FlowValidationException(Exception):
    pass


class PersistentCellValidationException(Exception):
    pass


class FlowIntegrationException(Exception):
    pass


class KeyNotUniqueException(Exception):
    def __init__(self, key):
        self.key = key
