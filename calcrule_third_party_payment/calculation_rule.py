import logging
import operator
from uuid import UUID

from django.contrib.contenttypes.models import ContentType
from django.db.models import Q, Subquery
from django.utils.translation import gettext as _


from calcrule_third_party_payment.config import (
    CLASS_RULE_PARAM_VALIDATION,
    CONTEXTS,
    DESCRIPTION_CONTRIBUTION_VALUATION,
    FROM_TO,
    INTEGER_PARAMETERS,
    NONE_INTEGER_PARAMETERS,
)
from calcrule_third_party_payment.converters import (
    ClaimsToBillConverter,
    ClaimToBillItemConverter,
)
from calcrule_third_party_payment.utils import (
    check_bill_exist,
    claim_batch_valuation,
    get_hospital_level_filter,
)
from claim.models import ClaimItem, ClaimService
from claim_batch.services import (
    get_hospital_claim_filter,
    update_claim_valuated,
    update_claim_indexed_remunerated
)
from contribution_plan.models import PaymentPlan
from contribution_plan.utils import obtain_calcrule_params
from core import datetime
from core.abs_calculation_rule import AbsStrategy
from core.models import User
from invoice.services import BillService
from location.models import HealthFacility
from product.models import Product

logger = logging.getLogger(__name__)


class ThirdPartyPaymentCalculationRule(AbsStrategy):
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

    @classmethod
    def active_for_object(
        cls, instance, context, type="account_payable", sub_type="third_party_payment"
    ):
        return (
            instance.__class__.__name__ == "PaymentPlan"
            and context in CONTEXTS
            and cls.check_calculation(instance)
        )

    @classmethod
    def check_calculation(cls, instance):
        class_name = instance.__class__.__name__
        match = False
        if class_name == "ABCMeta":
            match = UUID(str(cls.uuid)) == UUID(str(instance.uuid))
        if class_name == "PaymentPlan":
            match = UUID(str(cls.uuid)) == UUID(str(instance.calculation))
        elif class_name == "BatchRun":
            # BatchRun → Product or Location if no prodcut
            match = cls.check_calculation(instance.location)
        elif class_name == "HealthFacility":
            #  HF → location
            match = cls.check_calculation(instance.location)
        elif class_name == "Location":
            #  location → ProductS (Product also related to Region if the location is a district)
            if instance.type in ["D", "R"]:
                products = Product.objects.filter(
                    location=instance, validity_to__isnull=True
                )
                for product in products:
                    if cls.check_calculation(product):
                        match = True
                        break
        elif class_name == "Claim":
            #  claim → claim product
            products = cls.__get_products_from_claim(claim=instance)
            # take the MAX Product id from item and services
            if len(products) > 0:
                product = max(products, key=operator.attrgetter("id"))
                match = cls.check_calculation(product)
        elif class_name == "Product":
            # if product → paymentPlans
            payment_plans = PaymentPlan.objects.filter(
                benefit_plan=instance, is_deleted=False
            )
            for pp in payment_plans:
                if cls.check_calculation(pp):
                    match = True
                    break
        return match

    @classmethod
    def calculate(cls, instance, **kwargs):
        context = kwargs.get("context", None)
        if instance.__class__.__name__ == "PaymentPlan":
            if context == "BatchPayment":
                cls.convert_batch(instance, **kwargs)
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
        if class_name is not None:
            model_class = ContentType.objects.filter(model__iexact=class_name).first()
            if model_class:
                model_class = model_class.model_class()
                list_class = list_class + [
                    f.remote_field.model.__name__
                    for f in model_class._meta.fields
                    if (
                        f.get_internal_type() == "ForeignKey"
                        and f.remote_field.model.__name__ != "User"
                    )
                ]
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
                    results = cls._convert_claims(instance, **kwargs)
            results["user"] = kwargs.get("user", None)
            BillService.bill_create(convert_results=results)
        return results

    @classmethod
    def convert_batch(cls, instance, work_data=None, **kwargs):
        pp_params = obtain_calcrule_params(
            instance, INTEGER_PARAMETERS, NONE_INTEGER_PARAMETERS
        )
        work_data = cls.filter_work_data(work_data, pp_params)
        logger.debug(f"creating bill for br {work_data['created_run']}")
        if work_data:
            user = User.objects.filter(
                i_user__id=work_data["created_run"].audit_user_id
            ).first()
            # create queryset based on provided params
            claim_queryset = work_data["claims"]
            claim_br_hf_list = HealthFacility.objects.filter(
                id__in=Subquery(
                    claim_queryset.values_list("health_facility", flat=True).distinct()
                )
            )
            for cbh in claim_br_hf_list:
                claim_queryset_by_br_hf = claim_queryset.filter(health_facility=cbh)
                # take all claims related to the same HF and batch_run to convert to bill
                cls.run_convert(
                    instance=claim_queryset_by_br_hf,
                    convert_to="Bill",
                    user=user,
                    health_facility=cbh,
                    work_data=work_data,
                    **kwargs,
                )
            update_claim_indexed_remunerated(
                claim_queryset,
                work_data["created_run"],
            )

    @classmethod
    def _process_batch_valuation(cls, instance, work_data=None, **kwargs):
        pp_params = obtain_calcrule_params(
            instance, INTEGER_PARAMETERS, NONE_INTEGER_PARAMETERS
        )
        work_data["pp_params"] = pp_params
        # manage the in/out patient params
        work_data = cls.filter_work_data(work_data, pp_params)
        claim_batch_valuation(instance, work_data)
        update_claim_valuated(work_data["claims"], work_data["created_run"])

    @staticmethod
    def filter_work_data(work_data, pp_params):
        product = work_data.get("product")
        work_data["claims"] = (
            work_data["claims"]
            .filter(get_hospital_level_filter(pp_params))
            .filter(
                get_hospital_claim_filter(
                    product.ceiling_interpretation, pp_params["claim_type"]
                )
            )
        )
        work_data["items"] = (
            work_data["items"]
            .filter(get_hospital_level_filter(pp_params, prefix="claim__"))
            .filter(
                get_hospital_claim_filter(
                    product.ceiling_interpretation, pp_params["claim_type"], "claim__"
                )
            )
        )
        work_data["services"] = (
            work_data["services"]
            .filter(get_hospital_level_filter(pp_params, prefix="claim__"))
            .filter(
                get_hospital_claim_filter(
                    product.ceiling_interpretation, pp_params["claim_type"], "claim__"
                )
            )
        )

        return work_data

    @classmethod
    def _convert_claims(cls, instance, **kwargs):
        products = cls.__get_products_from_claim_queryset(claim_queryset=instance)
        # take the MAX Product id from item and services
        if len(products) > 0:
            product = max(products, key=operator.attrgetter("id"))
            work_data = kwargs.get("work_data")
            health_facility = kwargs.get("health_facility")
            if work_data:
                batch_run = work_data.get("created_run")

                bill = ClaimsToBillConverter.to_bill_obj(
                    claims=instance,
                    product=product,
                    health_facility=health_facility,
                    batch_run=batch_run,
                )
                bill_line_items = []
                for claim in instance.all():
                    bill_line_item = ClaimToBillItemConverter.to_bill_line_item_obj(
                        claim=claim
                    )
                    bill_line_items.append(bill_line_item)
                    ClaimsToBillConverter.build_amounts(bill_line_item, bill)
            else:
                raise Exception(
                    _(
                        "no %s found, it is mandatory for this claim to bill converter"
                        % "batch_run"
                    )
                )

            return {
                "bill_data": bill,
                "bill_data_line": bill_line_items,
                "type_conversion": "claims queryset-bill",
            }

    @classmethod
    def __get_products_from_claim_queryset(cls, claim_queryset):
        return Product.objects.filter(
            Q(id__in=Subquery(claim_queryset.values("items__product")))
            | Q(id__in=Subquery(claim_queryset.values("services__product")))
        )

    @classmethod
    def __get_products_from_claim(cls, claim):
        products = []
        # get the clam product from claim item and claim services
        for svc_item in [ClaimItem, ClaimService]:
            claim_details = (
                svc_item.objects.filter(claim__id=claim.id)
                .filter(claim__validity_to__isnull=True)
                .filter(validity_to__isnull=True)
            )
            for cd in claim_details:
                if cd.product:
                    products.append(cd.product)
        return products
