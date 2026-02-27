# src/connectors/__init__.py
from . import hansgrohe, dallmer

CONNECTORS = {
    "hansgrohe": hansgrohe,
    "dallmer": dallmer,
}