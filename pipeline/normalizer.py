# pipeline/normalizer.py
import httpx
import fitz  # PyMuPDF

def normalize_html(html: str) -> str:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)

def normalize_pdf(pdf_bytes: bytes) -> str:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    return "\n".join(page.get_text() for page in doc)

def normalize(content: str | bytes, content_type: str) -> str:
    if content_type == "pdf":
        return normalize_pdf(content)
    return normalize_html(content)