import sys
import os

# Add root directory to python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipeline.normalizer import normalize

def test_html_parser():
    print("--- HTML Parser Test ---")
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
    
    print("Original HTML length:", len(sample_html))
    
    clean_text = normalize(sample_html, "html")
    print("\nParsed Output:")
    print("--------------------------------------------------")
    print(clean_text)
    print("--------------------------------------------------")
    
    assert "Menu | Home" not in clean_text, "Navigation wasn't stripped!"
    assert "console.log" not in clean_text, "Scripts weren't stripped!"
    assert "TechCorp Announces" in clean_text, "Main content is missing!"
    print("\n✅ HTML parsing worked correctly.")

def test_pdf_parser():
    print("\n--- PDF Parser Test ---")
    print("To test PDF parsing, you need a local PDF file.")
    print("You can run something like:")
    print("with open('sample.pdf', 'rb') as f:")
    print("    text = normalize(f.read(), 'pdf')")
    print("    print(text)")

if __name__ == "__main__":
    test_html_parser()
    test_pdf_parser()
