from enum import Enum


class Classification(Enum):
    FORECAST = 1
    CURRENT = 2
    BACKCAST = 3


class Granularity(Enum):
    TEN_MIN = 3
    FIFTEEN_MIN = 4
    HOUR = 5
    DAY = 6
    MONTH = 7
    YEAR = 8


class TimeZone(Enum):
    UTC = 0
    CET = 1


class Activity(Enum):
    GENERATION = 1
    CONSUMPTION = 2


class DataType(Enum):
    ALL = 0
    WIND = 1
    SOLAR = 2
    BIOGAS = 3
    HEATPUMP = 4
    COFIRING = 8
    GEOTHERMAL = 9
    OTHER = 10
    WASTE = 11
    BIOOIL = 12
    BIOMASS = 13
    WOOD = 14
    WIND_OFFSHORE = 17
    FOSSIL_GAS_POWER = 18
    FOSSIL_HARD_COAL = 19
    NUCLEAR = 20
    WASTE_POWER = 21
    WIND_OFFSHORE_B = 22
    NATURAL_GAS = 23
    BIOMETHANE = 24
    BIOMASS_POWER = 25
    OTHER_POWER = 26
    ELECTRICITY_MIX = 27
    GAS_MIX = 28
    GAS_DISTRIBUTION = 31
    WKK_TOTAL = 35
    SOLAR_THERMAL = 50
    WIND_OFFSHORE_C = 51
    INDUSTRIAL_CONSUMERS_GAS_COMBINATION = 53
    INDUSTRIAL_CONSUMERS_POWER_GAS_COMBINATION = 54
    LOCAL_DISTRIBUTION_COMPANIES_COMBINATION = 55
    ALL_CONSUMING_GAS = 56


class Point(Enum):
    NETHERLANDS = 0
    GRONINGEN = 1
    FRIESLAND = 2
    DRENTHE = 3
    OVERRIJSEL = 4
    FLEVOLAND = 5
    GELDERLAND = 6
    UTRECHT = 7
    NOORD_HOLLAND = 8
    ZUID_HOLLAND = 9
    ZEELAND = 10
    NOORD_BRABANT = 11
    LIMBURG = 12
    OFFSHORE = 14
    WINDPARK_LUCHTERDUINEN = 28
    WINDPARK_PRINCESS_AMALIA = 29
    WINDPARK_EGMOND_AAN_ZEE = 30
    WINDPARK_GEMINI = 31
    WINDPARK_BORSELE_I_II = 33
    WINDPARK_BORSELE_III_IV = 34
    WINDPARK_HOLLANDSE_KUST_ZUID = 35
    WINDPARK_HOLLANDSE_KUST_NOORD = 35
