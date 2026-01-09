import json
import xml.etree.ElementTree as ET
from pathlib import Path


class DataExporter:
    def __init__(self, data):
        self.data = data

    def export(self, format: str, filepath: str):
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        if format == 'json':
            self.export_json(filepath)
        elif format == 'xml':
            self.export_xml(filepath)
        else:
            raise ValueError("Unsupported format. Use 'json' or 'xml'.")

    def export_json(self, filepath: Path):
        def convert(obj):
            if isinstance(obj, (int, float, str, type(None))):
                return obj
            elif isinstance(obj, dict):
                return {k: convert(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert(v) for v in obj]
            else:
                return float(obj)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(convert(self.data), f, ensure_ascii=False, indent=4)

    def export_xml(self, filepath: Path):
        root = ET.Element("results")

        for row in self.data:
            item = ET.SubElement(root, "item")
            for key, value in row.items():
                child = ET.SubElement(item, key)
                child.text = str(value)

        tree = ET.ElementTree(root)
        tree.write(filepath, encoding="utf-8", xml_declaration=True)
