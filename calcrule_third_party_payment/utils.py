import json
from django.contrib.contenttypes.models import ContentType
from django.db.models import Value, F, Sum, Q, Prefetch, Count, Subquery, OuterRef, FloatField
from django.db.models.functions import Coalesce
from claim.models import ClaimItem, Claim, ClaimService
from claim_batch.models import RelativeIndex, RelativeDistribution
from claim_batch.services import get_hospital_claim_filter, get_period
from invoice.models import BillItem
from location.models import HealthFacility
from product.models import Product

INTEGER_PARAMETERS = [
    "share_contribution",
    "distr_1", "distr_2", "distr_3", "distr_4", "distr_5", "distr_6",
    "distr_7", "distr_8", "distr_9", "distr_10", "distr_11", "distr_12",
]

NONE_INTEGER_PARAMETERS = ['hf_level_1',
                           'hf_sublevel_1',
                           'hf_level_2',
                           'hf_sublevel_2',
                           'hf_level_3',
                           'hf_sublevel_3',
                           'hf_level_4',
                           'hf_sublevel_4']


def check_bill_exist(instance, convert_to, **kwargs):
    if instance.__class__.__name__ == "QuerySet":
        queryset_model = instance.model
        if queryset_model.__name__ == "Claim":
            claim = instance.first()
            content_type = ContentType.objects.get_for_model(claim.__class__)
            bills = BillItem.objects.filter(line_type=content_type, line_id=claim.id)
            if bills.count() == 0:
                return True


# TODO move it to contribution plan
def obtain_calcrule_params(payment_plan) -> dict:
    # obtaining payment plan params saved in payment plan json_ext fields
    pp_params = payment_plan.json_ext
    if isinstance(pp_params, str):
        pp_params = json.loads(pp_params)
    if pp_params:
        pp_params = pp_params["calculation_rule"] if "calculation_rule" in pp_params else None
    # correct empty string values

    for key in INTEGER_PARAMETERS:
        if key in pp_params.items():
            value = pp_params.items()[f'{key}']
            if value == "":
                pp_params[f'{key}'] = 0
            else:
                pp_params[f'{key}'] = int(value)
        else:
            pp_params[f'{key}'] = 0

    for key in NONE_INTEGER_PARAMETERS:
        if key not in pp_params.items():
            pp_params[f'{key}'] = None

    return pp_params


def claim_batch_valuation(payment_plan, work_data):
    """ update the service and item valuated amount """

    work_data["periodicity"] = payment_plan.periodicity
    product = work_data["product"]
    items = work_data["items"]
    services = work_data["services"]
    start_date = work_data["start_date"]
    end_date = work_data["end_date"]
    claims = work_data["claims"]
    pp_params = work_data["pp_params"]
    # Sum up all item and service amount
    value = 0
    value_items = None
    value_services = None
    index = 0

    # if there is no configuration the relative index will be set to 100 %
    if start_date is not None:

        value_items = items.aggregate(sum=Sum('price_adjusted'))
        value_services = services.aggregate(sum=Sum('price_adjusted'))
        if sum in value_items:
            value += value_items['sum']
        if sum in value_services:
            value += value_services['sum']

        index = get_relative_price_rate(value, pp_params, work_data)

        # update the item and services
        items.update(price_valuated=F('price_adjusted') * index)
        services.update(price_valuated=F('price_adjusted') * index)


def is_hospital_claim(product, claim):
    if product.ceiling_interpretation == Product.CEILING_INTERPRETATION_HOSPITAL:
        return claim.health_facility.level == HealthFacility.LEVEL_HOSPITAL
    else:
        return claim.date_to is not None and claim.date_to > claim.date_from


def get_hospital_level_filter(pp_params, prefix=''):
    Qterm = Q()
    hf = '%shealth_facility' % prefix

    # if no filter all would be taken into account
    if pp_params['hf_level_1']:
        if pp_params['hf_sublevel_1']:
            Qterm |= (Q(('%s__Level' % hf, pp_params['hf_level_1'])) & Q(
                ('%s__SubLevel' % hf, pp_params['hf_sublevel_1'])))
        else:
            Qterm |= Q(('%s__Level' % hf, pp_params['hf_level_1']))
    if pp_params['hf_level_2']:
        if pp_params['hf_sublevel_2']:
            Qterm |= (Q(('%s__Level' % hf, pp_params['hf_level_2'])) & Q(
                ('%s__SubLevel' % hf, pp_params['hf_sublevel_2'])))
        else:
            Qterm |= Q(('%s__Level' % hf, pp_params['hf_level_2']))
    if pp_params['hf_level_3']:
        if pp_params['hf_sublevel_3']:
            Qterm |= (Q(('%s__Level' % hf, pp_params['hf_level_3'])) & Q(
                ('%s__SubLevel' % hf, pp_params['hf_sublevel_3'])))
        else:
            Qterm |= Q(('%s__Level' % hf, pp_params['hf_level_3']))
    if pp_params['hf_level_4']:
        if pp_params['hf_sublevel_4']:
            Qterm |= (Q(('%s__Level' % hf, pp_params['hf_level_4'])) & Q(
                ('%s__SubLevel' % hf, pp_params['hf_sublevel_4'])))
        else:
            Qterm |= Q(('%s__Level' % hf, pp_params['hf_level_4']))
    return Qterm


# to be moded in product services
def create_index(product, index, index_type, period_type, period_id):
    index = RelativeIndex()
    index.product = product
    index.type = index_type
    index.care_type = period_type
    index.period = period_id
    from core.utils import TimeUtils
    index.calc_date = TimeUtils.now()
    index.save()


# might be added in product service
def get_relative_price_rate(value, pp_params, work_data):
    # index = (allocated_contribution * distr) / value
    # get distr for the current month
    allocated_contributions = work_data["allocated_contributions"]
    if value > 0 and allocated_contributions > 0 and 'distr_%i' % pp_params['end_date'].month in pp_params:
        distr = pp_params['distr_%i' % pp_params['end_date'].month]
        index = (allocated_contributions * distr) / value
        period_type, period_id = get_period(work_data['start_date'], work_data['end_date'])
        create_index(pp_params['product'], index, pp_params['claim_type'], period_type, period_id)
        return index
    else:
        return 1



