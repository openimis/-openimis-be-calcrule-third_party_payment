from django.contrib.contenttypes.models import ContentType
from invoice.models import BillItem
from invoice.services import BillService, BillLineItemService


def check_bill_exist(instance, convert_to, **kwargs):
    if instance.__class__.__name__ == "QuerySet":
        queryset_model = instance.model
        if queryset_model.__name__ == "Claim":
            claim = instance.first()
            content_type = ContentType.objects.get_for_model(claim.__class__)
            bills = BillItem.objects.filter(line_type=content_type, line_id=claim.id)
            if bills.count() == 0:
                return True


def save_bill_in_db(convert_results):
    if 'bill_data' in convert_results and 'bill_data_line' in convert_results:
        user = convert_results['user']
        # save in database this invoice and invoice line item
        bill_line_items = convert_results['bill_data_line']
        bill_service = BillService(user=user)
        bill_line_item_service = BillLineItemService(user=user)
        result_bill = bill_service.create(convert_results['bill_data'])
        if result_bill["success"] is True:
            bill_update = {
                "id": result_bill["data"]["id"],
                "amount_net": 0,
                "amount_total": 0,
                "amount_discount": 0
            }
            for bill_line_item in bill_line_items:
                bill_line_item["bill_id"] = result_bill["data"]["id"]
                result_bill_line = bill_line_item_service.create(bill_line_item)
                if result_bill_line["success"] is True:
                    bill_update["amount_net"] += float(result_bill_line["data"]["amount_net"])
                    bill_update["amount_total"] += float(result_bill_line["data"]["amount_total"])
                    bill_update["amount_discount"] += 0 if result_bill_line["data"]["discount"] else result_bill_line["data"]["discount"]
            bill_service.update(bill_update)
