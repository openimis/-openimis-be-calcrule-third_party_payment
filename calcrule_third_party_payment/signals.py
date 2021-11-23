from core.signals import bind_service_signal
from core.service_signals import ServiceSignalBindType
from calcrule_third_party_payment.calculation_rule import ThirdPartyPaymentCalculationRule


def bind_service_signals():
    bind_service_signal(
        'bill_creation_from_calculation',
        ThirdPartyPaymentCalculationRule.convert_batch,
        bind_type=ServiceSignalBindType.AFTER
    )
