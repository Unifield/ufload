import cloud
import httpfile

__version__ = '0.1'

# null progress, can be overridden by importers
def _progress(p):
    pass

progress = _progress
