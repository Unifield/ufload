import cloud
import db

__version__ = '0.3'

# null progress, can be overridden by importers
def _progress(p):
    pass

progress = _progress
