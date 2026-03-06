# src/connectors/__init__.py
from . import hansgrohe, dallmer, tece, aco, viega

CONNECTORS = {
    "hansgrohe": hansgrohe,
    "dallmer": dallmer,
    "tece": tece,
    "aco": aco,
    "viega": viega,
}