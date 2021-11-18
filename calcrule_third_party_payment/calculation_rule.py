import operator

from .apps import AbsCalculationRule
from .config import CLASS_RULE_PARAM_VALIDATION, \
    DESCRIPTION_CONTRIBUTION_VALUATION, FROM_TO
from core.signals import *
from core import datetime
from django.contrib.contenttypes.models import ContentType
from claim.models import ClaimItem, ClaimService
from contribution_plan.models import PaymentPlan
from product.models import Product
from calcrule_third_party_payment.converters import ClaimsToBillConverter, ClaimToBillItemConverter


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
    def active_for_object(cls, instance, context, type, sub_type):
        return instance.__class__.__name__ == "PaymentPlan" \
               and context in ["submit"] \
               and cls.check_calculation(instance)

    @classmethod
    def check_calculation(cls, instance):
        class_name = instance.__class__.__name__
        match = False
        if class_name == "PaymentPlan":
            match = cls.uuid == instance.calculation
        elif class_name == "BatchRun":
            # BatchRun → Prodcut or Location if no prodcut
            match = cls.check_calculation(instance.location)
        elif class_name == "HealthFacility":
            #  HF → location
            match = cls.check_calculation(instance.location)
        elif class_name == "Location":
            #  location → ProductS (Product also related to Region if the location is a district)
            if instance.type == "D":
                products = Product.objects.filter(location=instance, validity_to__isnull=True)
                for product in products:
                    if cls.check_calculation(product):
                        match = True
                        break
            match = cls.check_calculation(instance.location)
        elif class_name == "Claim":
            #  claim → claim product
            products = cls.__get_products_from_claim(claim=instance)
            # take the MAX Product id from item and services
            if len(products) > 0:
                product = max(products, key=operator.attrgetter('id'))
                match = cls.check_calculation(product)
        elif class_name == "Product":
            # if product → paymentPlans
            payment_plans = PaymentPlan.objects.filter(product=instance, is_deleted=False)
            for pp in payment_plans:
                if cls.check_calculation(pp):
                    match = True
                    break
        return match

    @classmethod
    def calculate(cls, instance, **kwargs):
        class_name = instance.__class__.__name__
        if instance.__class__.__name__ == "PaymentPlan":
            date_from = kwargs.get('date_from', None)
            date_to = kwargs.get('date_to', None)
            product_id = kwargs.get('product_id', None)
            location = kwargs.get('location', None)
            # TODO get all “processed“ claims that should be evaluated with fee for service
            #  that matches args (should replace the batch run)
            pass

    @classmethod
    def get_linked_class(cls, sender, class_name, **kwargs):
        list_class = []
        if class_name != None:
            model_class = ContentType.objects.filter(model=class_name).first()
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
    @register_service_signal('convert_to_bill')
    def convert(cls, instance, convert_to, **kwargs):
        # check from signal before if invoice already exist for instance
        results = {}
        signal = REGISTERED_SERVICE_SIGNALS['convert_to_bill']
        results_check_invoice_exist = signal.signal_results['before'][0][1]
        if results_check_invoice_exist:
            convert_from = instance.__class__.__name__
            if convert_from == "QuerySet":
                # get the model name from queryset
                convert_from = instance.model.__name__
                if convert_from == "Claim":
                    results = cls._convert_claims(instance)
            results['user'] = kwargs.get('user', None)
        # after this method signal is sent to invoice module to save invoice data in db
        return results

    @classmethod
    def convert_batch(cls, **kwargs):
        pass

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
