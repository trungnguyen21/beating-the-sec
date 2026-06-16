# pipeline/normalizer.py
import re
import fitz  # PyMuPDF
from bs4 import BeautifulSoup


def normalize_press_release(html: str) -> str:
    """
    Clean an SEC press release HTML page down to plain text.
    Handles XBRL inline tags and converts financial tables to pipe-delimited rows.
    """
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "nav", "footer", "header",
                     "img", "svg", "link", "meta", "noscript"]):
        tag.decompose()

    # XBRL inline tags wrap numbers with metadata — unwrap to keep only the value
    for tag in soup.find_all(re.compile(r'^ix:')):
        tag.unwrap()

    # Convert tables to readable pipe-delimited rows so AI workers see the numbers
    for table in soup.find_all("table"):
        rows = []
        for tr in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if any(cells):
                rows.append(" | ".join(cells))
        table.replace_with("\n".join(rows) + "\n")

    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()


def split_sections(text: str) -> dict[str, str]:
    """
    Split normalized press release text into logical sections.
    Each AI worker receives only the slice most relevant to its task.
    """
    sections: dict[str, str] = {
        "header": "",
        "financials": "",
        "guidance": "",
        "full": text,
    }

    for line in text.split("\n"):
        lower = line.lower()
        if any(k in lower for k in ["outlook", "guidance", "fiscal 20", "next quarter", "expect"]):
            sections["guidance"] += line + "\n"
        elif any(k in lower for k in ["revenue", "earnings per", "net income", "gross margin",
                                       "operating income", "diluted"]):
            sections["financials"] += line + "\n"
        elif any(k in lower for k in ["said ", "ceo", "chairman", "record"]):
            sections["header"] += line + "\n"

    return sections


def normalize_pdf(pdf_bytes: bytes) -> str:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    return "\n".join(page.get_text() for page in doc)


def normalize(content: str | bytes, content_type: str) -> str:
    if content_type == "pdf":
        return normalize_pdf(content)
    return normalize_press_release(content)
