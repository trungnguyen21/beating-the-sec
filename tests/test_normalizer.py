import pytest

from pipeline.normalizer import normalize


def test_html_parser():
    sample_html = """
    <html>
        <head>
            <style>body { background: white; }</style>
            <script>console.log('tracker');</script>
        </head>
        <body>
            <nav>Menu | Home | About</nav>
            <h1>TechCorp Announces Q3 2025 Earnings</h1>
            <p>Net income was <strong>$1.5 Billion</strong>.</p>
            <footer>Copyright 2025 TechCorp</footer>
        </body>
    </html>
    """

    clean_text = normalize(sample_html, "html")

    assert "Menu | Home" not in clean_text, "Navigation wasn't stripped!"
    assert "console.log" not in clean_text, "Scripts weren't stripped!"
    assert "TechCorp Announces" in clean_text, "Main content is missing!"


@pytest.mark.skip(reason="requires a local sample PDF file — not bundled in this repo")
def test_pdf_parser():
    with open("sample.pdf", "rb") as f:
        text = normalize(f.read(), "pdf")
    assert text
