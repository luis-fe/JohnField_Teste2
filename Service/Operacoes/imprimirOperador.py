import pandas as pd
from reportlab.lib.pagesizes import portrait
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
import tempfile
from reportlab.graphics.barcode import code128
import qrcode
from Service.Operacoes import Operadores
import ConexaoPostgreMPL

def criar_formulario(saida_pdf, nomeOperador):
    # Configurações das etiquetas e colunas
    label_width = 21.0 * cm
    label_height = 29.7 * cm

    # Criar o PDF e ajustar o tamanho da página para retrato com tamanho personalizado
    custom_page_size = portrait((label_width, label_height))
    c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)

    # Título centralizado na primeira página
    c.setFont("Helvetica-Bold", 21)
    title = 'FORMULÁRIO OPERADORES'
    c.drawString(5.0 * cm, 28.7 * cm, title)

    # Título da página
    c.setFont("Helvetica-Bold", 11)
    title = 'Página 1'
    c.drawString(18.0 * cm, 28.7 * cm, title)

    # Desenhar linhas horizontais e verticais
    c.setLineWidth(2)
    inicioLinha = 5.3
    alturaLinha = 28.4

    for l in range(3):
        for l2 in range(8):
            c.line(0 * cm, (alturaLinha - (l2 * 5.5)) * cm, 21 * cm, (alturaLinha - (l2 * 5.5)) * cm)
        c.line(inicioLinha * (l + 1) * cm, 28.4 * cm, inicioLinha * (l + 1) * cm, 0.9 * cm)
    # Variáveis de controle para iteração
    inicio = 0.4
    inicio2 = 0.4
    inicio3 = 0.4
    inicio4 = 0.4
    inicio5 = 0.4
    pagina = 0
    pg = 1

    for i, (operacao, categoria) in enumerate(zip(nomeOperador, nomeOperador), start=1):
        iPagina = i - pagina
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
            qr_filename = temp_qr_file.name
            if iPagina < 5:
                consulta = Operadores.ConsultarOperadores()
                codigoOperador = consulta[consulta['nomeOperador'] == operacao].reset_index()
                print(codigoOperador)
                codigoOperador = str(codigoOperador['codOperador'][0])
                # Título da operação
                c.setFont("Helvetica-Bold", 12)
                title = operacao
                c.drawString(inicio * cm, 24.2 * cm, title)



                # Categoria da operação
                Label(c, codigoOperador, inicio, 23.7)


                # Gerar QR code
                qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
                qr.add_data(str(codigoOperador))
                qr.make(fit=True)
                qr_img = qr.make_image(fill_color="black", back_color="white")
                qr_img.save(qr_filename)
                c.drawImage(qr_filename, (inicio + (0.20 * iPagina)) * cm, 25.2 * cm, width=2.7 * cm, height=2.70 * cm)
                inicio += 5.3
            elif iPagina < 9:
                consulta = Operadores.ConsultarOperadores()
                codigoOperador = consulta[consulta['nomeOperador'] == operacao].reset_index()
                print(codigoOperador)
                codigoOperador = str(codigoOperador['codOperador'][0])
                # Título da operação
                c.setFont("Helvetica-Bold", 12)
                title = operacao
                c.drawString(inicio2 * cm, (24.2 - 5.5) * cm, title)

                # Categoria da operação
                Label(c, codigoOperador, inicio2, 23.7 - 5.5)

                # Gerar QR code
                qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
                qr.add_data(str(codigoOperador))
                qr.make(fit=True)
                qr_img = qr.make_image(fill_color="black", back_color="white")
                qr_img.save(qr_filename)
                i2 = i - 4
                c.drawImage(qr_filename, (inicio2 + (0.20 * i2)) * cm, (25.2 - 5.5) * cm, width=2.7 * cm, height=2.70 * cm)
                inicio2 += 5.3
            elif iPagina < 13:
                consulta = Operadores.ConsultarOperadores()
                codigoOperador = consulta[consulta['nomeOperador'] == operacao].reset_index()
                print(codigoOperador)
                codigoOperador = str(codigoOperador['codOperador'][0])
                # Título da operação
                c.setFont("Helvetica-Bold", 12)
                title = operacao
                c.drawString(inicio3 * cm, (24.2 - (5.5 * 2)) * cm, title)

                # Categoria da operação
                Label(c, codigoOperador, inicio3, 23.7 - (5.5 * 2))

                # Gerar QR code
                qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
                qr.add_data(str(codigoOperador))
                qr.make(fit=True)
                qr_img = qr.make_image(fill_color="black", back_color="white")
                qr_img.save(qr_filename)
                i2 = i - 8
                c.drawImage(qr_filename, (inicio3 + (0.20 * i2)) * cm, (25.2 - (5.5 * 2)) * cm, width=2.7 * cm, height=2.70 * cm)
                inicio3 += 5.3
            elif iPagina < 17:
                consulta = Operadores.ConsultarOperadores()
                codigoOperador = consulta[consulta['nomeOperador'] == operacao].reset_index()
                print(codigoOperador)
                codigoOperador = str(codigoOperador['codOperador'][0])
                # Título da operação
                c.setFont("Helvetica-Bold", 12)
                title = operacao
                c.drawString(inicio4 * cm, (24.2 - (5.5 * 3)) * cm, title)

                # Categoria da operação
                Label(c, codigoOperador, inicio3, 23.7 - (5.5 * 3))

                # Gerar QR code
                qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
                qr.add_data(str(codigoOperador))
                qr.make(fit=True)
                qr_img = qr.make_image(fill_color="black", back_color="white")
                qr_img.save(qr_filename)
                i2 = i - 12
                c.drawImage(qr_filename, (inicio4 + (0.20 * i2)) * cm, (25.2 - (5.5 * 3)) * cm, width=2.7 * cm, height=2.70 * cm)
                inicio4 += 5.3
            elif iPagina < 21:
                consulta = Operadores.ConsultarOperadores()
                codigoOperador = consulta[consulta['nomeOperador'] == operacao].reset_index()
                print(codigoOperador)
                codigoOperador = str(codigoOperador['codOperador'][0])
                # Título da operação
                c.setFont("Helvetica-Bold", 12)
                title = operacao
                c.drawString(inicio5 * cm, (24.2 - (5.5 * 4)) * cm, title)

                # Categoria da operação
                Label(c, codigoOperador, inicio3, 23.7 - (5.5 * 4))

                # Gerar QR code
                qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
                qr.add_data(str(codigoOperador))
                qr.make(fit=True)
                qr_img = qr.make_image(fill_color="black", back_color="white")
                qr_img.save(qr_filename)
                i2 = i - 16
                c.drawImage(qr_filename, (inicio5 + (0.20 * i2)) * cm, (25.2 - (5.5 * 4)) * cm, width=2.7 * cm, height=2.70 * cm)
                inicio5 += 5.3
            else:
                pagina += 21
                inicio = 0.4
                inicio2 = 0.4
                inicio3 = 0.4
                inicio4 = 0.4
                inicio5 = 0.4
                pg += 1
                c.showPage()

                # Título centralizado
                c.setFont("Helvetica-Bold", 21)
                title = 'FORMULÁRIO OPERADORES'
                c.drawString(5.0 * cm, 28.7 * cm, title)

                # Título da página
                c.setFont("Helvetica-Bold", 11)
                title = f'Página {pg}'
                c.drawString(18.0 * cm, 28.7 * cm, title)

                # Desenhar linhas horizontais e verticais
                c.setLineWidth(2)
                inicioLinha = 5.3
                alturaLinha = 28.4

                for l in range(3):
                    for l2 in range(8):
                        c.line(0 * cm, (alturaLinha - (l2 * 5.5)) * cm, 21 * cm, (alturaLinha - (l2 * 5.5)) * cm)
                    c.line(inicioLinha * (l + 1) * cm, 28.4 * cm, inicioLinha * (l + 1) * cm, 0.9 * cm)
                consulta = Operadores.ConsultarOperadores()
                codigoOperador = consulta[consulta['nomeOperador'] == operacao].reset_index()
                print(codigoOperador)
                codigoOperador = str(codigoOperador['codOperador'][0])
                # Título da operação
                c.setFont("Helvetica-Bold", 12)
                title = operacao
                c.drawString(inicio * cm, 24.2 * cm, title)

                # Categoria da operação
                Label(c, codigoOperador, inicio, 23.7)

                # Gerar QR code
                qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
                qr.add_data(str(codigoOperador))
                qr.make(fit=True)
                qr_img = qr.make_image(fill_color="black", back_color="white")
                qr_img.save(qr_filename)
                c.drawImage(qr_filename, (inicio + (0.20 * 0)) * cm, 25.2 * cm, width=2.7 * cm, height=2.70 * cm)
                inicio += 5.3

    c.save()

def Label(canvas, nome, posicaoInicial, altura):
    # Título da categoria
    canvas.setFont("Helvetica-Bold", 12)
    title = nome
    canvas.drawString(posicaoInicial * cm, altura * cm, title)
