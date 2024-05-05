import pandas as pd
from reportlab.lib.pagesizes import landscape
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
import tempfile



def criar_pdf(saida_pdf):
    # Configurações das etiquetas e colunas
    label_width = 7.5 * cm
    label_height = 1.8 * cm

    # Criar o PDF e ajustar o tamanho da página para paisagem com tamanho personalizado
    custom_page_size = landscape((label_width, label_height))

    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
        c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)

        c.save()
    return pd.DataFrame([{'Mensagem':True}])