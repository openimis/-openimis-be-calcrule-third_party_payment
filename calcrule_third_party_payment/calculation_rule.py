import operator

from django.contrib.contenttypes.models import ContentType

from calcrule_third_party_payment.apps import AbsCalculationRule
from calcrule_third_party_payment.config import (
    CLASS_RULE_PARAM_VALIDATION,
    DESCRIPTION_CONTRIBUTION_VALUATION,
    FROM_TO,
    INTEGER_PARAMETERS,
    NONE_INTEGER_PARAMETERS,
    CONTEXTS
)
from calcrule_third_party_payment.utils import (
    check_bill_exist,
    claim_batch_valuation,
    get_hospital_level_filter
)
from claim.models import (
    Claim,
    ClaimItem,
    ClaimService
)
from claim_batch.services import (
    get_hospital_claim_filter,
    update_claim_valuated
)
from contribution_plan.models import PaymentPlan
from contribution_plan.utils import obtain_calcrule_params
from core.signals import *
from core import datetime
from invoice.services import BillService

from product.models import Product
from calcrule_third_party_payment.converters import ClaimsToBillConverter, ClaimToBillItemConverter
from core.models import User


class ThirdPartyPaymentCalculationRule(AbsCalculationRule):
    version = 1
    uuid = "0a1b6d54-eef4-4ee6-ac47-2a99cfa5e9a8"
    calculation_rule_name = "payment: fee for service"
    description = DESCRIPTION_CONTRIBUTION_VALUATION
    impacted_class_parameter = CLASS_RULE_PARAM_VALIDATION
    date_valid_from = datetime.datetime(2000, 1, 1)
    date_valid_to = None
    status = "active"
    from_to = FROM_TO
    type = "account_payable"
    sub_type = "third_party_payment"

    signal_get_rule_name = Signal(providing_args=[])
    signal_get_rule_details = Signal(providing_args=[])
    signal_get_param = Signal(providing_args=[])
    signal_get_linked_class = Signal(providing_args=[])
    signal_calculate_event = Signal(providing_args=[])
    signal_convert_from_to = Signal(providing_args=[])

    @classmethod
    def ready(cls):
        now = datetime.datetime.now()
        condition_is_valid = (now >= cls.date_valid_from and now <= cls.date_valid_to) \
            if cls.date_valid_to else (now >= cls.date_valid_from and cls.date_valid_to is None)
        if condition_is_valid:
            if cls.status == "active":
                # register signals getParameter to getParameter signal and getLinkedClass ot getLinkedClass signal
                cls.signal_get_rule_name.connect(cls.get_rule_name, dispatch_uid="on_get_rule_name_signal")
                cls.signal_get_rule_details.connect(cls.get_rule_details, dispatch_uid="on_get_rule_details_signal")
                cls.signal_get_param.connect(cls.get_parameters, dispatch_uid="on_get_param_signal")
                cls.signal_get_linked_class.connect(cls.get_linked_class, dispatch_uid="on_get_linked_class_signal")
                cls.signal_calculate_event.connect(cls.run_calculation_rules, dispatch_uid="on_calculate_event_signal")
                cls.signal_convert_from_to.connect(cls.run_convert, dispatch_uid="on_convert_from_to")

    @classmethod
    def active_for_object(cls, instance, context, type='account_payable', sub_type='third_party_payment'):
        return instance.__class__.__name__ == "PaymentPlan" \
               and context in CONTEXTS \
               and cls.check_calculation(instance)

    @classmethod
    def check_calculation(cls, instance):
        class_name = instance.__class__.__name__
        match = False
        if class_name == "ABCMeta":
            match = str(cls.uuid) == str(instance.uuid)
        elif class_name == "PaymentPlan":
            match = cls.uuid == str(instance.calculation)
        elif class_name == "BatchRun":
            # BatchRun → Product or Location if no prodcut
            match = cls.check_calculation(instance.location)
        elif class_name == "HealthFacility":
            #  HF → location
            match = cls.check_calculation(instance.location)
        elif class_name == "Location":
            #  location → ProductS (Product also related to Region if the location is a district)
            if instance.type in ["D", "R"]:
                products = Product.objects.filter(location=instance, validity_to__isnull=True)
                for product in products:
                    if cls.check_calculation(product):
                        match = True
                        break
        elif class_name == "Claim":
            #  claim → claim product
            products = cls.__get_products_from_claim(claim=instance)
            # take the MAX Product id from item and services
            if len(products) > 0:
                product = max(products, key=operator.attrgetter('id'))
                match = cls.check_calculation(product)
        elif class_name == "Product":
            # if product → paymentPlans
            payment_plans = PaymentPlan.objects.filter(benefit_plan=instance, is_deleted=False)
            for pp in payment_plans:
                if cls.check_calculation(pp):
                    match = True
                    break
        return match

    @classmethod
    def calculate(cls, instance, **kwargs):
        context = kwargs.get('context', None)
        class_name = instance.__class__.__name__
        if instance.__class__.__name__ == "PaymentPlan":
            if context == "BatchPayment":
                work_data = kwargs.get('work_data', None)
                cls.convert_batch(work_data=work_data)
                return "conversion finished 'fee for service'"
            elif context == "BatchValuate":
                cls._process_batch_valuation(instance, **kwargs)
                return "valuation finished 'fee for service'"
            elif context == "IndividualPayment":
                pass
            elif context == "IndividualValuation":
                pass

    @classmethod
    def get_linked_class(cls, sender, class_name, **kwargs):
        list_class = []
        if class_name != None:
            model_class = ContentType.objects.filter(model__iexact=class_name).first()
            if model_class:
                model_class = model_class.model_class()
                list_class = list_class + \
                             [f.remote_field.model.__name__ for f in model_class._meta.fields
                              if f.get_internal_type() == 'ForeignKey' and f.remote_field.model.__name__ != "User"]
        else:
            list_class.append("Calculation")
        # because we have calculation in PaymentPlan
        #  as uuid - we have to consider this case
        if class_name == "PaymentPlan":
            list_class.append("Calculation")
        return list_class

    @classmethod
    def convert(cls, instance, convert_to, **kwargs):
        results = {}
        if check_bill_exist(instance, convert_to):
            convert_from = instance.__class__.__name__
            if convert_from == "QuerySet":
                # get the model name from queryset
                convert_from = instance.model.__name__
                if convert_from == "Claim":
                    results = cls._convert_claims(instance)
            results['user'] = kwargs.get('user', None)
            BillService.bill_create(convert_results=results)
        return results

    @classmethod
    def convert_batch(cls, **kwargs):
        work_data = kwargs.get('work_data', None)
        if work_data:
            user = User.objects.filter(i_user__id=work_data['created_run'].audit_user_id).first()
            # create queryset based on provided params
            claim_queryset = Claim.objects.filter(validity_to=None, batch_run=work_data['created_run'], health_facility__isnull=False)
            claim_br_hf_list = list(claim_queryset.values('batch_run', 'health_facility').distinct())
            for cbh in claim_br_hf_list:
                claim_queryset_by_br_hf = Claim.objects.filter(
                    batch_run__id=cbh["batch_run"], health_facility__id=cbh["health_facility"]
                )
                # take all claims related to the same HF and batch_run to convert to bill
                cls.run_convert(instance=claim_queryset_by_br_hf, convert_to='Bill', user=user)

    @classmethod
    def _process_batch_valuation(cls, instance, **kwargs):
        work_data = kwargs.get('work_data', None)
        product = work_data["product"]
        pp_params = obtain_calcrule_params(instance, INTEGER_PARAMETERS, NONE_INTEGER_PARAMETERS)
        work_data["pp_params"] = pp_params
        # manage the in/out patient params
        work_data["claims"] = work_data["claims"].filter(get_hospital_level_filter(pp_params)) \
            .filter(get_hospital_claim_filter(product.ceiling_interpretation, pp_params['claim_type']))
        work_data["items"] = work_data["items"].filter(get_hospital_level_filter(pp_params, prefix='claim__')) \
            .filter(get_hospital_claim_filter(product.ceiling_interpretation, pp_params['claim_type'], 'claim__'))
        work_data["services"] = work_data["services"].filter(get_hospital_level_filter(pp_params, prefix='claim__')) \
            .filter(get_hospital_claim_filter(product.ceiling_interpretation, pp_params['claim_type'], 'claim__'))
        claim_batch_valuation(instance, work_data)
        update_claim_valuated(work_data['claims'], work_data['created_run'])

    @classmethod
    def _convert_claims(cls, instance):
        products = cls.__get_products_from_claim_queryset(claim_queryset=instance)
        # take the MAX Product id from item and services
        if len(products) > 0:
            product = max(products, key=operator.attrgetter('id'))
            bill = ClaimsToBillConverter.to_bill_obj(claims=instance, product=product)
            bill_line_items = []
            for claim in instance.all():
                bill_line_item = ClaimToBillItemConverter.to_bill_line_item_obj(claim=claim)
                bill_line_items.append(bill_line_item)
            return {
                'bill_data': bill,
                'bill_data_line': bill_line_items,
                'type_conversion': 'claims queryset-bill'
            }

    @classmethod
    def __get_products_from_claim_queryset(cls, claim_queryset):
        products = []
        # get the clam product from claim item and claim services
        for claim in claim_queryset.all():
            for svc_item in [ClaimItem, ClaimService]:
                claim_details = svc_item.objects \
                    .filter(claim__id=claim.id) \
                    .filter(claim__validity_to__isnull=True) \
                    .filter(validity_to__isnull=True)
                for cd in claim_details:
                    if cd.product:
                        products.append(cd.product)
        return products

    @classmethod
    def __get_products_from_claim(cls, claim):
        products = []
        # get the clam product from claim item and claim services
        for svc_item in [ClaimItem, ClaimService]:
            claim_details = svc_item.objects \
                .filter(claim__id=claim.id) \
                .filter(claim__validity_to__isnull=True) \
                .filter(validity_to__isnull=True)
            for cd in claim_details:
                if cd.product:
                    products.append(cd.product)
        return products
