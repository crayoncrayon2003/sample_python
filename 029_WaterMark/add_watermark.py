"""
繰り返し透かし（ウォーターマーク）をPDFに追加するスクリプト

使い方:
  pip install pypdf reportlab fonttools otf2ttf
  python add_watermark.py input.pdf output.pdf
"""

import sys
import os
import io
import tempfile
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ===== カスタマイズ設定 =====
WATERMARK_TEXT = "confidential！再配布禁止！！"
FONT_SIZE = 28
ANGLE = 45
OPACITY = 0.25
SPACING_X = 400
SPACING_Y = 250
COLOR = (0.5, 0.5, 0.5)
# ===========================

TTC_PATH = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
TTF_INDEX = 0  # 0 = Noto Sans CJK JP


def convert_cff_to_ttf(ttc_path, index):
    """TTCからフォントを取り出し、CFF→TTFに変換して一時ファイルパスを返す"""
    from fontTools.ttLib import TTCollection
    from otf2ttf.cli import otf_to_ttf  # ← 修正箇所

    # TTCから対象フォントを取り出す
    ttc = TTCollection(ttc_path)
    font = ttc[index]

    # CFF → TTF アウトライン変換
    otf_to_ttf(font)

    # 一時TTFファイルに保存
    tmp_ttf = tempfile.NamedTemporaryFile(suffix=".ttf", delete=False)
    tmp_ttf.close()
    font.save(tmp_ttf.name)

    print(f"CFF→TTF変換完了: {tmp_ttf.name}")
    return tmp_ttf.name


def register_japanese_font():
    """日本語フォントを登録"""
    ttf_path = convert_cff_to_ttf(TTC_PATH, TTF_INDEX)
    pdfmetrics.registerFont(TTFont("JapaneseFont", ttf_path))
    print("日本語フォント登録完了")
    return "JapaneseFont", ttf_path


def create_watermark_page(page_width, page_height, font_name):
    """指定サイズのページに繰り返し透かしを描画したPDFバイト列を返す"""
    packet = io.BytesIO()
    c = canvas.Canvas(packet, pagesize=(page_width, page_height))

    c.setFillColorRGB(*COLOR, alpha=OPACITY)
    c.setFont(font_name, FONT_SIZE)

    margin = max(page_width, page_height)

    x = -margin
    while x < page_width + margin:
        y = -margin
        while y < page_height + margin:
            c.saveState()
            c.translate(x, y)
            c.rotate(ANGLE)
            c.drawCentredString(0, 0, WATERMARK_TEXT)
            c.restoreState()
            y += SPACING_Y
        x += SPACING_X

    c.save()
    packet.seek(0)
    return packet


def add_watermark(input_path, output_path):
    """PDFに透かしを追加して保存する"""
    print(f"読み込み中: {input_path}")
    reader = PdfReader(input_path)
    writer = PdfWriter()

    font_name, ttf_tmp = register_japanese_font()

    total = len(reader.pages)
    print(f"全{total}ページに透かしを追加中...")

    for i, page in enumerate(reader.pages):
        width = float(page.mediabox.width)
        height = float(page.mediabox.height)

        watermark_packet = create_watermark_page(width, height, font_name)
        watermark_page = PdfReader(watermark_packet).pages[0]

        page.merge_page(watermark_page)
        writer.add_page(page)

        print(f"  ページ {i+1}/{total} 完了")

    with open(output_path, "wb") as f:
        writer.write(f)

    os.unlink(ttf_tmp)
    print(f"\n完了！出力ファイル: {output_path}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("使い方: python add_watermark.py input.pdf output.pdf")
        sys.exit(1)

    input_pdf = sys.argv[1]
    output_pdf = sys.argv[2]

    if not os.path.exists(input_pdf):
        print(f"エラー: ファイルが見つかりません: {input_pdf}")
        sys.exit(1)

    add_watermark(input_pdf, output_pdf)