# src/connectors/__init__.py
from . import hansgrohe, dallmer, tece

CONNECTORS = {
    "hansgrohe": hansgrohe,
    "dallmer": dallmer,
    "tece": tece,
}