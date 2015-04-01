from sorl.thumbnail.conf import settings, defaults as default_settings
from sorl.thumbnail.helpers import tokey, serialize
from sorl.thumbnail.images import DummyImageFile, ImageFile
from sorl.thumbnail.kvstores.base import add_prefix
from sorl.thumbnail.helpers import serialize, deserialize
from sorl.thumbnail import default
from sorl.thumbnail.parsers import parse_geometry
from sorl.thumbnail.base import ThumbnailBackend

from sorlery.tasks import create_thumbnail


class QueuedThumbnailBackend(ThumbnailBackend):
    """
    Queue thumbnail generation with django-celery.
    """
    def get_thumbnail(self, file_, geometry_string, **options):
        source = ImageFile(file_)

        for key, value in self.default_options.iteritems():
            options.setdefault(key, value)

        for key, attr in self.extra_options:
            value = getattr(settings, attr)
            if value != getattr(default_settings, attr):
                options.setdefault(key, value)

        # Generate a name for the thumbnail
        name = self._get_thumbnail_filename(source, geometry_string, options)

        # See if we've got a hit in the cache
        thumbnail = ImageFile(name, default.storage)
        cached = default.kvstore.get(thumbnail)
        if cached:
            return cached

        job = create_thumbnail.delay(file_, geometry_string, options, name)

        # Return source file if thumbnail not exists
        source_image = ImageFile(file_.name, default.storage)
        width = geometry_string.split('x')[0]
        height = geometry_string.split('x')[1] if len(geometry_string.split('x')) > 1 else ''
        source_image._size = [width, height]
        return source_image