from django.contrib.contenttypes.models import ContentType

from claim.models import ClaimItem, ClaimService


class ClaimToBillItemConverter(object):

    @classmethod
    def to_bill_line_item_obj(cls, claim):
        bill_line_item = {}
        cls.build_line_fk(bill_line_item, claim)
        cls.build_dates(bill_line_item, claim)
        cls.build_code(bill_line_item, claim)
        cls.build_description(bill_line_item, claim)
        cls.build_details(bill_line_item, claim)
        cls.build_quantity(bill_line_item)
        cls.build_unit_price(bill_line_item, claim)
        cls.build_discount(bill_line_item, claim)
        # cls.build_tax(bill_line_item)
        cls.build_amounts(bill_line_item)
        return bill_line_item

    @classmethod
    def build_line_fk(cls, bill_line_item, claim):
        bill_line_item["line_id"] = claim.id
        bill_line_item["line_type"] = ContentType.objects.get_for_model(claim)

    @classmethod
    def build_dates(cls, bill_line_item, claim):
        bill_line_item["date_valid_from"] = claim.date_from
        bill_line_item["date_valid_to"] = claim.date_to

    @classmethod
    def build_code(cls, bill_line_item, claim):
        bill_line_item["code"] = claim.code

    @classmethod
    def build_description(cls, bill_line_item, claim):
        bill_line_item["description"] = f"{claim.icd.code} {claim.icd.name}"

    @classmethod
    def build_details(cls, bill_line_item, claim):
        details = []
        for svc_item in [ClaimItem, ClaimService]:
            claim_details = (
                svc_item.objects.filter(claim__id=claim.id)
                .filter(claim__validity_to__isnull=True)
                .filter(validity_to__isnull=True)
            )
            for claim_detail in claim_details:
                name = (
                    claim_detail.item.name
                    if claim_detail.__class__.__name__ == "ClaimItem"
                    else claim_detail.service.name
                )
                details.append(
                    {
                        "name": name,
                        "quantity": f"{claim_detail.qty_provided}",
                        "quantity_approved": f"{claim_detail.qty_approved}",
                        "price": f"{claim_detail.price_asked}",
                        "price_approved": f"{claim_detail.price_approved}",
                    }
                )
        bill_line_item["details"] = {"claim_details": details}

    @classmethod
    def build_quantity(cls, bill_line_item):
        bill_line_item["quantity"] = 1

    @classmethod
    def build_unit_price(cls, bill_line_item, claim):
        bill_line_item["unit_price"] = claim.claimed or claim.remunerated

    @classmethod
    def build_discount(cls, bill_line_item, claim):
        if claim.claimed and claim.remunerated:
            if claim.claimed != claim.remunerated:
                bill_line_item["deduction"] = claim.claimed - claim.remunerated

    @classmethod
    def build_tax(cls, bill_line_item):
        bill_line_item["tax_rate"] = None
        bill_line_item["tax_analysis"] = None

    @classmethod
    def build_amounts(cls, bill_line_item):
        if bill_line_item["unit_price"]:
            bill_line_item["amount_net"] = (
                bill_line_item["quantity"] * bill_line_item["unit_price"]
            )
        else:
            bill_line_item["amount_net"] = 0
        if "deduction" in bill_line_item:
            bill_line_item["amount_net"] = (
                bill_line_item["amount_net"] - bill_line_item["deduction"]
            )
        bill_line_item["amount_total"] = bill_line_item["amount_net"]
