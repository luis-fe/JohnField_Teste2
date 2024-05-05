import pandas as pd
from reportlab.lib.pagesizes import portrait  # Alteração aqui
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
import tempfile

def criar_pdf(saida_pdf):
    # Configurações das etiquetas e colunas
    label_width = 21.0 * cm
    label_height = 29.7 * cm

    # Criar o PDF e ajustar o tamanho da página para retrato com tamanho personalizado
    custom_page_size = portrait((label_width, label_height))  # Alteração aqui

    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
        c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)

        # Título centralizado
        c.setFont("Helvetica-Bold", 24)
        title = 'ORDEM DE PRODUCAO'
        c.drawString(3.9 * cm, 28.8 * cm, title)

        c.save()
    return pd.DataFrame([{'Mensagem': True}])

criar_pdf('teste.pdf')
