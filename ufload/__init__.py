import cloud
import httpfile

__version__ = '1.0'

# null progress, can be overridden by importers
def _progress(p):
    pass

progress = _progress
