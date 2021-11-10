# openimis-be-calcrule-third_party_payment_py
This repository holds the files of the openIMIS backend calculation rule contribution legacy reference module.
It is dedicated to be deployed as a module of [openimis-be_py](https://github.com/openimis/openimis-be_py). It is an 
extended part of calculation module [openimis-be-calculation_py](https://github.com/openimis/openimis-be-calculation_py) 
and therefore that module is dependent on this core calculation module. This module contains additional calculation rules. 

## Models
  - None (using no database approach for CalculationRule) - Calculation Rule is saved by defining class 
    extending the ABSCalculationClass from core module.
    