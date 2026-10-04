"""Short-horizon forecast of magnetic-latitude / MLT |dB/dt| from L1 solar wind.

Training labels are delayed 30–90 minutes after the L1 sample. Features never
include future L1. This is not a city or substation GIC model.
"""

__version__ = "0.1.0"
