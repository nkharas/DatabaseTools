# Author : Nick Kharas


class InputError(Exception):
    """
    https://docs.python.org/3/tutorial/errors.html
    Exception raised for errors in the input.
    """

    def __init__(self, message):
        """
        :param message: Message that gets displayed when the exception is raised
        """
        self.message = message
        
    def __str__(self):
        return repr(self.message)
