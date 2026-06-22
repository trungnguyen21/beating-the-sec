import re
from models.model_detectors import ItemPipeline

ITEM_PIPELINE: dict[str, ItemPipeline] = {
    "2.02": ItemPipeline.EARNINGS,
    "5.02": ItemPipeline.LEADERSHIP_CHANGE,
    "2.01": ItemPipeline.ACQUISITION,
    "4.02": ItemPipeline.RESTATEMENT,
    "1.03": ItemPipeline.BANKRUPTCY,
    "2.05": ItemPipeline.RESTRUCTURING,
}

def classify_items(items_desc: str) -> list[str]:
    """Map items-desc field from SEC RSS to pipeline names. Returns only known/actionable items."""
    item_numbers = re.findall(r'\d+\.\d+', items_desc)
    return [ITEM_PIPELINE[i].value for i in item_numbers if i in ITEM_PIPELINE]
