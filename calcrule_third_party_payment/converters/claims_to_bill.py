from django.contrib.contenttypes.models import ContentType
from django.utils.translation import gettext as _

from invoice.apps import InvoiceConfig
from invoice.models import Bill


class ClaimsToBillConverter(object):

    @classmethod
    def to_bill_obj(cls, claims, product, health_facility, batch_run):
        bill = {}
        # single bill = queryset of claims with the same batch run id and health facility
        # get the first claim because all claims from queryset has the same batch run id
        if not batch_run:
            raise Exception(
                _(
                    "no %s found, it is mandatory for this claim to bill converter"
                    % "batch_run"
                )
            )
        # get the first claim because all claims from queryset has the same health facility
        if not health_facility:
            raise Exception(
                _(
                    "no %s found, it is mandatory for this claim to bill converter"
                    % "health_facility"
                )
            )
        cls.build_subject(batch_run, bill)
        cls.build_thirdparty(health_facility, bill)
        cls.build_code(health_facility, product, batch_run, bill)
        cls.build_date_dates(batch_run, bill)
        # cls.build_tax_analysis(bill)
        cls.build_currency(bill)
        cls.build_status(bill)
        cls.build_terms(product, bill)
        cls.build_init_amounts(bill)
        return bill

    @classmethod
    def build_subject(cls, batch_run, bill):
        bill["subject_type"] = ContentType.objects.get_for_model(batch_run)
        bill["subject"] = batch_run

    @classmethod
    def build_thirdparty(cls, health_facility, bill):
        # get the first claim because all claims from queryset has the same health facility
        bill["thirdparty"] = health_facility
        bill["thirdparty_type"] = ContentType.objects.get_for_model(health_facility)

    @classmethod
    def build_code(cls, health_facility, product, batch_run, bill):
        bill["code"] = (
            f""
            f"IV-{product.code}"
            f"-{health_facility.code}"
            f"-{batch_run.run_date.strftime('%Y-%m')}"
        )

    @classmethod
    def build_date_dates(cls, batch_run, bill):
        from core import datetimedelta

        bill["date_due"] = batch_run.run_date + datetimedelta(days=30)
        bill["date_bill"] = batch_run.run_date
        bill["date_valid_from"] = batch_run.run_date
        # TODO - explain/clarify meaning of 'validity to' of this field
        # bill["date_valid_to"] = batch_run.expiry_date

    @classmethod
    def build_tax_analysis(cls, bill):
        bill["tax_analysis"] = None

    @classmethod
    def build_currency(cls, bill):
        bill["currency_tp_code"] = InvoiceConfig.default_currency_code
        bill["currency_code"] = InvoiceConfig.default_currency_code

    @classmethod
    def build_status(cls, bill):
        bill["status"] = Bill.Status.VALIDATED.value

    @classmethod
    def build_terms(cls, product, bill):
        bill["terms"] = product.name

    @classmethod
    def build_amounts(cls, line_item, bill_update):

        bill_update["amount_net"] += line_item["amount_net"]
        bill_update["amount_total"] += line_item["amount_total"]
        # bill_update["amount_discount"] += 0 if "discount" in  line_item or not line_item["discount"]
        # else line_item["discount"]
        # bill_update["amount_deduction"] += 0 if "deduction" in  line_item or not line_item["deduction"]
        # else line_item["deduction"]

    @classmethod
    def build_init_amounts(cls, bill_update):

        bill_update["amount_net"] = 0
        bill_update["amount_total"] = 0
        # bill_update["amount_discount"] += 0 if "discount" in  line_item or not line_item["discount"]
        # else line_item["discount"]
        # bill_update["amount_deduction"] += 0 if "deduction" in  line_item or not line_item["deduction"]
        # else line_item["deduction"]
