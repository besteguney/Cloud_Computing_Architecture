class ImageNotFound(Exception):
    def __init__(self, message=None):
        self.message = message
        super().__init__(message)

class ContainerNotFound(Exception):
    def __init__(self, message=None):
        self.message = message
        super().__init__(message)