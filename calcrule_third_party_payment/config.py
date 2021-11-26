CLASS_RULE_PARAM_VALIDATION = []

FROM_TO = [
        {"from": "QuerySet", "to": "Bill"},
        {"from": "Claim", "to": "BillItem"}
]

DESCRIPTION_CONTRIBUTION_VALUATION = F"" \
    F"This calculation will, for the selected level and product," \
    F" calculate how much the insurance need to" \
    F" pay the HF for the fee for service health financing"
