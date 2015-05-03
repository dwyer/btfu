import abc


class BlobStore(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_blob(self, blobref):
        """Should return the blob at the given ``blobref`` if it exists,
        otherwise ``None``.
        """

    @abc.abstractmethod
    def get_link(self, link):
        """Should return the blobref at the given ``link`` if it exists,
        otherwise ``None``.
        """

    @abc.abstractmethod
    def get_size(self, blobref):
        """Should return the blob's size if it exists, otherwise ``None``."""

    @abc.abstractmethod
    def has_blob(self, blobref):
        """Should return ``True`` if the blob exists, otherwise ``False``."""

    @abc.abstractmethod
    def put_blob(self, blob):
        """Should store the given ``blob`` and return its blobref."""

    @abc.abstractmethod
    def set_link(self, link, blobref):
        """Should set the value ``link`` to ``blobref``. If ``link`` is empty,
        generate one. If ``blobref`` is empty, delete ``link``. If both are
        empty, return ``None``. Otherwise, return either ``link`` or the link
        that was generated."""
