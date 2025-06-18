class ClientError(Exception):
    """Base class for client visible errors."""

class InvalidInputError(ClientError):
    """Raised when user input validation fails."""

