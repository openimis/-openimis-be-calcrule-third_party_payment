from django.apps import AppConfig

from calculation.apps import CALCULATION_RULES, read_all_calculation_rules

MODULE_NAME = "calcrule_third_party_payment"
DEFAULT_CFG = {}


class CalcruleThirdPartyPaymentConfig(AppConfig):
    name = MODULE_NAME

    def ready(self):
        from core.models import ModuleConfiguration

        ModuleConfiguration.get_or_default(MODULE_NAME, DEFAULT_CFG)
        read_all_calculation_rules(MODULE_NAME, CALCULATION_RULES)
