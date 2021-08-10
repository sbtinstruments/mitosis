# TODO: Add base exception class. E.g.: "MitosisError" or "FlowError".
# Derive all other exceptions from this class. This way, the user can "catch"
# all mitosis-related errors with a single `except` block. Should we add another
# error in the future, the very same `èxcept` block catches it as well.
# In other words: We have future-compatible user code.


class FlowValidationException(Exception):
    pass


class PersistentCellValidationException(Exception):
    pass


class FlowIntegrationException(Exception):
    pass


class KeyNotUniqueException(Exception):
    def __init__(self, key):
        self.key = key
