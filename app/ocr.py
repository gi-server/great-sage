"""
OCR and document text extraction.

Ported from Poneglyph's ocr_service/main.py — preserves the dual-strategy
approach (native PDF text → Tesseract fallback) and image preprocessing.
"""

from __future__ import annotations

import io
import logging
import os
import platform
from typing import TYPE_CHECKING

import pymupdf as fitz  # PyMuPDF
import pytesseract
from PIL import Image, ImageFilter

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.ocr")

# On Windows, Tesseract is installed to this path by default (UB Mannheim installer).
# On Linux/Docker (production), tesseract is on PATH — no override needed.
if platform.system() == "Windows":
    pytesseract.pytesseract.tesseract_cmd = (
        r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    )

ALLOWED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg"}


def is_allowed_file(filename: str) -> bool:
    """Check whether the filename extension is in the allowlist."""
    ext = os.path.splitext(filename.lower())[1]
    return ext in ALLOWED_EXTENSIONS


def preprocess_image(img: Image.Image) -> Image.Image:
    """Apply light preprocessing to improve OCR accuracy on scanned documents."""
    img = img.convert("L")  # Grayscale
    img = img.filter(ImageFilter.SHARPEN)  # Mild sharpening for blurry scans
    return img


def extract_text(file_content: bytes, filename: str, settings: "Settings") -> str:
    """
    Extract text from a PDF or image file.

    PDF strategy:
      1. Try native text extraction per page (digital PDFs — instant).
      2. Fall back to rendering + Tesseract for image-only pages.

    Image strategy:
      Preprocess → Tesseract.

    Returns the extracted text, truncated to settings.max_ocr_text_chars.
    Raises on unrecoverable errors.
    """
    filename_lower = filename.lower()
    extracted_parts: list[str] = []

    if filename_lower.endswith(".pdf"):
        extracted_parts = _extract_from_pdf(file_content)
    else:
        extracted_parts = _extract_from_image(file_content)

    full_text = "\n\n".join(extracted_parts).strip()

    # Truncate to configured maximum
    if len(full_text) > settings.max_ocr_text_chars:
        full_text = full_text[: settings.max_ocr_text_chars]

    logger.info("OCR complete — extracted %d characters", len(full_text))
    return full_text


def _extract_from_pdf(content: bytes) -> list[str]:
    """Extract text from each page of a PDF document."""
    parts: list[str] = []
    pdf_document = fitz.open(stream=content, filetype="pdf")

    try:
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)

            # Strategy 1: native text extraction (digital PDFs)
            native_text = page.get_text("text").strip()
            if native_text:
                parts.append(native_text)
                continue

            # Strategy 2: render to image → Tesseract
            pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            img = preprocess_image(img)
            text = pytesseract.image_to_string(img, lang="eng", config="--psm 3")
            if text.strip():
                parts.append(text.strip())
    finally:
        pdf_document.close()

    return parts


def _extract_from_image(content: bytes) -> list[str]:
    """Extract text from a single image file (PNG/JPG)."""
    parts: list[str] = []
    img = Image.open(io.BytesIO(content)).convert("RGB")
    img = preprocess_image(img)
    text = pytesseract.image_to_string(img, lang="eng", config="--psm 3")
    if text.strip():
        parts.append(text.strip())
    return parts


def check_tesseract() -> bool:
    """Return True if Tesseract is reachable."""
    try:
        version = pytesseract.get_tesseract_version()
        logger.debug("Tesseract version: %s", version)
        return True
    except Exception:
        return False
