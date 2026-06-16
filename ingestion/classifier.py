import re

ITEM_PIPELINE: dict[str, str] = {
    '2.02': 'earnings',
    '5.02': 'leadership_change',
    '2.01': 'acquisition',
    '4.02': 'restatement',
    '1.03': 'bankruptcy',
    '2.05': 'restructuring',
}

def classify_items(items_desc: str) -> list[str]:
    """Map items-desc field from SEC RSS to pipeline names. Returns only known/actionable items."""
    item_numbers = re.findall(r'\d+\.\d+', items_desc)
    return [ITEM_PIPELINE[i] for i in item_numbers if i in ITEM_PIPELINE]
