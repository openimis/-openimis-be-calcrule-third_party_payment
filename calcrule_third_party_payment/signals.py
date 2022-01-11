"""
from core.signals import bind_service_signal
from core.service_signals import ServiceSignalBindType
from calcrule_third_party_payment.calculation_rule import ThirdPartyPaymentCalculationRule


def bind_service_signals():
    bind_service_signal(
        'signal_after_claim_batch_module_process_batch_service',
        ThirdPartyPaymentCalculationRule.convert_batch_old,
        bind_type=ServiceSignalBindType.AFTER
    )
"""
