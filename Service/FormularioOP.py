import pandas as pd
from reportlab.lib.pagesizes import portrait  # Alteração aqui
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
import tempfile
from reportlab.graphics import barcode
import qrcode
from Service import ClientesJohnField, OP_JonhField
def criar_pdf(saida_pdf, codCliente, codOP):

    consulta = BuscarCliente(codCliente)
    nomeCliente = consulta['nomeCliente'][0]

    # Configurações das etiquetas e colunas
    label_width = 21.0 * cm
    label_height = 29.7 * cm

    # Criar o PDF e ajustar o tamanho da página para retrato com tamanho personalizado
    custom_page_size = portrait((label_width, label_height))  # Alteração aqui

    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
        c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)
        qr_filename = temp_qr_file.name

        # Título centralizado
        c.setFont("Helvetica-Bold", 21)
        title = 'ORDEM DE PRODUCAO'
        c.drawString(7.0 * cm, 28.8 * cm, title)

        # Título centralizado
        c.setFont("Helvetica", 14)
        title = codOP
        c.drawString(6.8 * cm, 28.8 * cm, title)


        # Título centralizado
        c.setFont("Helvetica-Bold", 14)
        title = 'OP:'
        c.drawString(5.8 * cm, 27.7 * cm, title)

        # Título centralizado
        c.setFont("Helvetica-Bold", 14)
        title = 'CATEGORIA:'
        c.drawString(10.2 * cm, 27.7 * cm, title)

        # Título centralizado
        c.setFont("Helvetica-Bold", 14)
        title = 'CLIENTE:'
        c.drawString(4.4 * cm, 27.0 * cm, title)

        # Título centralizado
        c.setFont("Helvetica", 14)
        title = nomeCliente
        c.drawString(6.8 * cm, 27.0 * cm, title)

        # Título centralizado
        c.setFont("Helvetica-Bold", 14)
        title = 'Descrição OP:'
        c.drawString(3.4 * cm, 26.3 * cm, title)


        # Adicionando cabecalho de cores
        c.setFont("Helvetica-Bold", 14)
        title = 'CORES'
        c.drawString(1.2 * cm, 23.8 * cm, title)

        # Inserir uma linha
        c.setLineWidth(1.3)  # Definir a largura da linha em 1 ponto
        c.line(0 * cm, 26.1 * cm, 27.8 * cm, 26.1 * cm)  # Desenhar uma linha
        c.line(1 * cm, 24.5 * cm, 20 * cm, 24.5 * cm)  # Desenhar uma linha
        c.line(1 * cm, 23.5 * cm, 20 * cm, 23.5 * cm)  # Desenhar uma linha

        # Inserir uma linha vertical
        c.line(3.5 * cm, 10 * cm, 3.5 * cm, 24.5 * cm)  # Desenhar uma linha vertical

        # Inserir uma imagem
        imagem_path = "Logo.png"  # Substitua pelo caminho da sua imagem
        c.drawImage(imagem_path, 0.2 * cm, 26.5 * cm, width=3 * cm, height=3 * cm)  # Posição e dimensões da imagem


        qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
        qr.add_data("cliente")  # Substitua pelo link desejado
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
        c.drawImage(qr_filename, 18.4 * cm, 27.0 * cm, width=2.2 * cm, height= 2.20 * cm)

        c.save()
    return pd.DataFrame([{'Mensagem': 'Gerado com sucessp','status':True}])

def BuscarCliente(codCliente):
    consulta = ClientesJohnField.ConsultaClientesEspecifico(codCliente)
    return consulta

def BucarOP(idOP):
    consulta = OP_JonhField.BuscandoOPEspecifica(idOP)
    return consulta
