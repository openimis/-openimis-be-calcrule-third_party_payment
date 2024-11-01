import calendar
import datetime
import decimal
from datetime import date, timedelta

from django.test import TestCase

from claim.models import Claim, ClaimDedRem
from claim.services import submit_claim, validate_and_process_dedrem_claim
from claim.test_helpers import (
    create_test_claim,
    create_test_claimitem,
    create_test_claimservice,
)
from claim_batch.services import do_process_batch
from contribution.test_helpers import create_test_payer, create_test_premium
from contribution_plan.tests.helpers import create_test_payment_plan
from core.services import create_or_update_core_user, create_or_update_interactive_user
from core.test_helpers import create_test_interactive_user
from insuree.test_helpers import create_test_insuree
from invoice.models import Bill, BillItem
from location.test_helpers import create_test_health_facility, create_test_location
from medical.test_helpers import create_test_item, create_test_service
from medical_pricelist.test_helpers import (
    add_item_to_hf_pricelist,
    add_service_to_hf_pricelist,
    create_test_item_pricelist,
    create_test_service_pricelist,
)
from policy.test_helpers import create_test_policy
from product.models import ProductItemOrService
from product.test_helpers import (
    create_test_product,
    create_test_product_item,
    create_test_product_service,
)

_TEST_USER_NAME = "test_batch_run"
_TEST_USER_PASSWORD = "test_batch_run"
_TEST_DATA_USER = {
    "username": _TEST_USER_NAME,
    "last_name": _TEST_USER_NAME,
    "password": _TEST_USER_PASSWORD,
    "other_names": _TEST_USER_NAME,
    "user_types": "INTERACTIVE",
    "language": "en",
    "roles": [1, 5, 9],
}


class BatchRunFeeForServiceTest(TestCase):
    def setUp(self) -> None:
        super(BatchRunFeeForServiceTest, self).setUp()
        i_user, i_user_created = create_or_update_interactive_user(
            user_id=None, data=_TEST_DATA_USER, audit_user_id=999, connected=False
        )
        user, user_created = create_or_update_core_user(
            user_uuid=None, username=_TEST_DATA_USER["username"], i_user=i_user
        )
        self.user = user

    def test_simple_batch(self):
        """
        This test creates a claim, submits it so that it gets dedrem entries,
        then submits a review rejecting part of it, then process the claim.
        It should not be processed (which was ok) but the dedrem should be deleted.
        """
        # create location
        test_region = create_test_location("R")
        test_district = create_test_location(
            "D", custom_props={"parent_id": test_region.id}
        )

        # Given
        insuree = create_test_insuree()
        self.assertIsNotNone(insuree)
        service = create_test_service("A", custom_props={"name": "test_simple_batch"})
        item = create_test_item("A", custom_props={"name": "test_simple_batch"})

        product = create_test_product(
            "CRTPP",
            custom_props={
                "name": "simplebatch",
                "lump_sum": 10_000,
                "location_id": test_region.id,
            },
        )
        create_test_payment_plan(
            product=product,
            calculation="0a1b6d54-eef4-4ee6-ac47-2a99cfa5e9a8",
            custom_props={
                "periodicity": 1,
                "date_valid_from": "2019-01-01",
                "date_valid_to": "2050-01-01",
                "json_ext": {
                    "calculation_rule": {
                        "hf_level_1": "H",
                        "hf_sublevel_1": "null",
                        "hf_level_2": "D",
                        "hf_sublevel_2": "null",
                        "hf_level_3": "C",
                        "hf_sublevel_3": "null",
                        "hf_level_4": "null",
                        "hf_sublevel_4": "null",
                        "distr_1": 100,
                        "distr_2": 100,
                        "distr_3": 100,
                        "distr_4": 100,
                        "distr_5": 100,
                        "distr_6": 100,
                        "distr_7": 100,
                        "distr_8": 100,
                        "distr_9": 100,
                        "distr_10": 100,
                        "distr_11": 100,
                        "distr_12": 100,
                        "claim_type": "B",
                    }
                },
            },
        )

        create_test_product_service(
            product,
            service,
            custom_props={"price_origin": ProductItemOrService.ORIGIN_RELATIVE},
        )
        create_test_product_item(
            product,
            item,
            custom_props={"price_origin": ProductItemOrService.ORIGIN_RELATIVE},
        )
        policy = create_test_policy(
            product,
            insuree,
            link=True,
            custom_props={
                "effective_date": date.today() - timedelta(days=200),
                "expiry_date": date.today() + timedelta(days=165),
                "start_date": date.today() - timedelta(days=200),
                "value": 1000,
            },
        )
        payer = create_test_payer()
        create_test_premium(
            policy_id=policy.id,
            custom_props={
                "payer_id": payer.id,
                "amount": 1000,
                "pay_date": date.today() - timedelta(days=200),
                "created_date": datetime.datetime.now() - timedelta(days=200),
            },
        )
        test_item_price_list = create_test_item_pricelist(test_region.id)
        test_service_price_list = create_test_service_pricelist(test_region.id)
        # create hf and attach item/services pricelist
        test_health_facility = create_test_health_facility(
            "HFT",
            test_district.id,
            custom_props={
                "services_pricelist_id": test_service_price_list.id,
                "items_pricelist_id": test_item_price_list.id,
            },
        )
        add_service_to_hf_pricelist(service, test_health_facility.id)
        add_item_to_hf_pricelist(item, test_health_facility.id)

        claim1 = create_test_claim(
            {
                "claimed": 500.0,
                "insuree_id": insuree.id,
                "health_facility_id": test_health_facility.id,
            }
        )
        service1 = create_test_claimservice(
            claim1,
            custom_props={
                "price_asked": 100,
                "service_id": service.id,
                "qty_provided": 2,
                "price_origin": ProductItemOrService.ORIGIN_RELATIVE,
            },
        )
        item1 = create_test_claimitem(
            claim1,
            "A",
            custom_props={
                "price_asked": 100,
                "item_id": item.id,
                "qty_provided": 3,
                "price_origin": ProductItemOrService.ORIGIN_RELATIVE,
            },
        )
        claim1.refresh_from_db()
        user = create_test_interactive_user()

        errors = submit_claim(claim1, user)
        errors += validate_and_process_dedrem_claim(claim1, user, True)
        claim1.process_stamp = claim1.validity_from
        claim1.save()
        self.assertEqual(len(errors), 0)
        self.assertEqual(
            claim1.status,
            Claim.STATUS_PROCESSED,
            "The claim has relative pricing, so should go to PROCESSED rather than VALUATED",
        )
        # Make sure that the dedrem was generated
        dedrem = ClaimDedRem.objects.filter(claim=claim1).first()
        self.assertIsNotNone(dedrem)
        self.assertEquals(dedrem.rem_g, 500)  # 100*2 + 100*3
        # renumerated should be Null
        self.assertEqual(claim1.remunerated, None)
        days_in_month = calendar.monthrange(
            claim1.validity_from.year, claim1.validity_from.month
        )[1]
        # When
        end_date = datetime.datetime(
            claim1.date_processed.year, claim1.date_processed.month, days_in_month
        )
        batch_run = do_process_batch(self.user.id_for_audit, test_region.id, end_date)
        claim1.refresh_from_db()
        item1.refresh_from_db()
        service1.refresh_from_db()

        self.assertEquals(claim1.status, Claim.STATUS_VALUATED)
        self.assertNotEqual(item1.price_valuated, item1.price_adjusted)
        self.assertNotEqual(service1.price_valuated, service1.price_adjusted)
        # based on calculation - should be 201.15 per item and service
        # therefore renumerated = 402.30
        expected_value = round(
            decimal.Decimal((1000 / 365 * days_in_month / 500 * 100)), 2
        )
        self.assertEqual(item1.price_valuated, expected_value)
        self.assertEqual(service1.price_valuated, expected_value)
        self.assertEqual(
            claim1.valuated, service1.price_valuated + item1.price_valuated
        )
        self.assertNotEqual(Bill.objects.filter(subject_id=batch_run.id).first(), None)
        self.assertNotEqual(
            BillItem.objects.filter(bill__subject_id=batch_run.id).first(), None
        )

        # tearDown
