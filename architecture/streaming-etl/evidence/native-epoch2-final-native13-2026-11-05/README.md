# Fixed-P2 final native process matrix

This directory preserves the Principal's independent execution of the thirteen
fixed-P2 process cases from the isolated `e0725b0` checkout. The source maps
include the generated `_build_info.py` input and were identical before and
after the run. The interrupted predecessor is retained as `UNKNOWN_EXIT` and
does not contribute to the verdict.

The scoped verdict covers N2.1's native process matrix. It does not accept all
of ADR-0043 or claim arbitrary DAG, cross-host, throughput-SLO, or HA support.
