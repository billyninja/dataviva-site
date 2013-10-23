# -*- coding: utf-8 -*-
"""
    Helpers
    ~~~~~~~
    
"""

''' Import statements '''
from decimal import Decimal, ROUND_HALF_UP

def d(x):
  return Decimal(x).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
