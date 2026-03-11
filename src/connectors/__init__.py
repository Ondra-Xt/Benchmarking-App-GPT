# src/connectors/__init__.py
from . import hansgrohe, dallmer, tece, aco, viega, geberit

CONNECTORS = {
    "hansgrohe": hansgrohe,
    "dallmer": dallmer,
    "tece": tece,
    "aco": aco,
    "viega": viega,
    "geberit": geberit,
}