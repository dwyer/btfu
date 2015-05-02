import abc


class BlobStore(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_blob(self, blobref):
        """Should return the blob if it exists, otherwise, None."""

    @abc.abstractmethod
    def get_size(self, blobref):
        """Should return the blob size if it exists, otherwise, None."""

    @abc.abstractmethod
    def has_blob(self, blobref):
        """Should return True if the blob exists, otherwise, False."""

    @abc.abstractmethod
    def put_blob(self, blob):
        """Should store the given blob and return its blobref."""
