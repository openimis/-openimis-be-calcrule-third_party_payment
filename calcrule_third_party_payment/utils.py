from django.contrib.contenttypes.models import ContentType
from django.db.models import Value, F, Sum, Q, Prefetch, Count

from claim.models import ClaimItem, Claim, ClaimService
from claim_batch.models import RelativeIndex, RelativeDistribution
from claim_batch.services import add_hospital_claim_date_filter, get_period
from invoice.models import BillItem
from location.models import HealthFacility
from product.models import Product


def check_bill_exist(instance, convert_to, **kwargs):
    if instance.__class__.__name__ == "QuerySet":
        queryset_model = instance.model
        if queryset_model.__name__ == "Claim":
            claim = instance.first()
            content_type = ContentType.objects.get_for_model(claim.__class__)
            bills = BillItem.objects.filter(line_type=content_type, line_id=claim.id)
            if bills.count() == 0:
                return True


def claim_batch_valuation(work_data):
    """ update the service and item valuated amount """
    allocated_contributions = work_data["allocated_contributions"]
    allocated_contributions_ip = work_data["allocated_contributions_ip"] if "allocated_contributions_ip" in work_data else None
    relative_price_g = work_data["relative_price_g"]
    product = work_data["product"]
    items = work_data["items"]
    services = work_data["services"]
    start_date = work_data["start_date"]
    start_date_ip = work_data["start_date_ip"]
    end_date = work_data["end_date"]
    claims = work_data["claims"]

    # Sum up all item and service amount
    value_hospital = 0
    value_non_hospital = 0
    index = 0
    index_ip = 0

    # if there is no configuration the relative index will be set to 100 %
    if start_date is not None or start_date_ip is not None:
        claims = add_hospital_claim_date_filter(claims, relative_price_g, start_date, start_date_ip, end_date,
                                                product.ceiling_interpretation)
        claims = claims.prefetch_related(
            Prefetch('items', queryset=ClaimItem.objects.filter(legacy_id__isnull=True).filter(product=product))
        ).prefetch_related(
            Prefetch('services', queryset=ClaimService.objects.filter(legacy_id__isnull=True).filter(product=product))
        )
        claims = list(claims.values_list('id', flat=True).distinct())
        claims = Claim.objects.filter(id__in=claims)
        # TODO to be replace by 2 queryset +  an Annotate add_hospital_claim_date_filter function could be used
        for claim in claims:
            for item in claim.items.all():
                if is_hospital_claim(product, claim):
                    value_hospital += item.price_valuated if item.price_valuated is not None else 0
                else:
                    value_non_hospital += item.price_valuated if item.price_valuated is not None else 0

            for service in claim.services.all():
                if is_hospital_claim(product, claim):
                    value_hospital += service.price_valuated if service.price_valuated is not None else 0
                else:
                    value_non_hospital += service.price_valuated if service.price_valuated is not None else 0
                # calculate the index based on product config

        # create i/o index OR in and out patien index
        if relative_price_g:
            index = get_relative_price_rate(product, 'B', start_date, end_date,
                                            allocated_contributions, value_non_hospital + value_hospital)
        else:
            if start_date_ip is not None:
                index_ip = get_relative_price_rate(product, 'I', start_date, end_date, allocated_contributions,
                                                   value_non_hospital + value_hospital)
            elif start_date is not None:
                index = get_relative_price_rate(product, 'O', start_date, end_date, allocated_contributions,
                                                value_non_hospital + value_hospital)

        # update the item and services
        # TODO check if a single UPDATE query is possible
        for claim in claims:
            for item in items:
                if is_hospital_claim(work_data["product"], item.claim) and (index > 0 or index_ip > 0):
                    item.amount_remunerated = item.price_valuated * (index if relative_price_g else index_ip)
                    item.save()
                elif index > 0:
                    item.amount_remunerated = item.price_valuated * index
                    item.save()
            for service in services:
                if is_hospital_claim(work_data["product"], service.claim) and (index > 0 or index_ip > 0):
                    service.amount_remunerated = service.price_valuated * (index if relative_price_g else index_ip)
                    service.save()
                elif index > 0:
                    service.amount_remunerated = service.price_valuated * index
                    service.save()


def is_hospital_claim(product, claim):
    if product.ceiling_interpretation == Product.CEILING_INTERPRETATION_HOSPITAL:
        return claim.health_facility.level == HealthFacility.LEVEL_HOSPITAL
    else:
        return claim.date_to is not None and claim.date_to > claim.date_from


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
def get_relative_price_rate(product, index_type, date_start, end_date, allocated_contributions, sum_r_items_services):
    period_type, period_id = get_period(date_start, end_date)
    rel_distribution = RelativeDistribution.objects.filter(product=product)\
        .filter(period=period_id)\
        .filter(type=period_type)\
        .filter(care_type=index_type)\
        .filter(legacy_id__isnull=True)
    if rel_distribution.count() > 0:
        rel_distribution = rel_distribution.first()
        rel_rate = rel_distribution.percent
        # TODO to be checked if rel_distribution perecentage is 0
        if rel_rate:
            index = (rel_rate * allocated_contributions) / sum_r_items_services
            create_index(product, index, index_type, period_type, period_id)
            return index
        else:
            return 1
    else:
        return 1


def update_claim_valuated(claims, batch_run):
    # 4 update the claim Total amounts if all Item and services got "valuated"
    # could be duplicates - distinct
    claims = claims.prefetch_related(Prefetch('items', queryset=ClaimItem.objects.filter(legacy_id__isnull=True))) \
        .prefetch_related(Prefetch('services', queryset=ClaimService.objects.filter(legacy_id__isnull=True)))
    claims = list(claims.values_list('id', flat=True).distinct())
    claims = Claim.objects.filter(id__in=claims)

    for claim in claims:
        remunerated_amount = 0
        for service in claim.services.all():
            remunerated_amount = service.remunerated_amount + remunerated_amount \
                if service.remunerated_amount else remunerated_amount
        for item in claim.items.all():
            remunerated_amount = item.remunerated_amount + remunerated_amount \
                if item.remunerated_amount else remunerated_amount
        if remunerated_amount > 0:
            claim.valuated = remunerated_amount
        claim.status = Claim.STATUS_VALUATED
        claim.batch_run = batch_run
        claim.save()
