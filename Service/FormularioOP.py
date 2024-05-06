import pandas as pd
from reportlab.lib.pagesizes import portrait  # Alteração aqui
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
import tempfile
from reportlab.graphics import barcode
import qrcode
from Service import ClientesJohnField, OP_JonhField, OP_Tam_Cor_JohnField
def criar_pdf(saida_pdf, codCliente, codOP):

    informacoes = OP_JonhField.ObterOP_EMAberto()
    informacoes = informacoes[(informacoes['codCliente'] == int(codCliente)) & (informacoes['codOP'] == codOP)].reset_index()
    informacoes.fillna('-',inplace=True)
    print(informacoes)
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
        c.drawString(6.7 * cm, 27.7 * cm, title)


        # Título centralizado
        c.setFont("Helvetica-Bold", 14)
        title = 'OP:'
        c.drawString(5.8 * cm, 27.7 * cm, title)

        # Título centralizado
        c.setFont("Helvetica-Bold", 13)
        title = 'CATEGORIA:'
        c.drawString(9.4 * cm, 27.7 * cm, title)

        # Título centralizado
        c.setFont("Helvetica", 12)
        title = informacoes['nomeCategoria'][0]
        c.drawString(12.3 * cm, 27.7 * cm, title)

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

        # Título centralizado
        c.setFont("Helvetica", 12)
        title = informacoes['descricaoOP'][0]
        c.drawString(6.9 * cm, 26.3 * cm, title)


        # Adicionando cabecalho de cores
        c.setFont("Helvetica-Bold", 14)
        title = 'CORES'
        c.drawString(1.2 * cm, 23.8 * cm, title)


        # Adicionando cabecalho de SUBTOTAIS
        c.setFont("Helvetica-Bold", 14)
        title = 'TOTAL'
        c.drawString(18.0 * cm, 23.8 * cm, title)



        # Título centralizado
        c.setFont("Helvetica-Bold", 11)
        title = 'DataCriacao OP:'
        c.drawString(14.8 * cm, 25.6 * cm, title)

        # Título centralizado
        c.setFont("Helvetica", 11)
        title = str(informacoes['dataCriacaoOP'][0])
        title = title[8:10]+'/'+title[5:7]+'/'+title[:4]+' '+title[11:16]
        c.drawString(17.6 * cm, 25.6 * cm, title)

        c.setLineWidth(2.5)  # Definir a largura da linha em 1 ponto
        c.line(1 * cm, 24.5 * cm, 20 * cm, 24.5 * cm)  # Desenhar uma linha
        c.line(1 * cm, 23.5 * cm, 20 * cm, 23.5 * cm)  # Desenhar uma linha


        # DELIMITACAO DA TABELA DE GRADES
        #_______________________________________________________________________________________________________
        # Inserir uma linha vertical
        c.setLineWidth(1.0)  # Definir a largura da linha em 1 ponto

        verificaGrade = OP_Tam_Cores(codOP,codCliente)
        print(verificaGrade)

        if verificaGrade['status'][0] == True:

            quantidadeCores = verificaGrade['descCor'].count()

            # Etapa: Avaliando a quantidade de cores para construir os limites
            #######################################################################
            if quantidadeCores <= 10:
                LimiteDaGrade(c, 10)
            else:
                LimiteDaGrade(c, 5.2)
            # Fim Etapa
            #########################################################################

            inicioCores = 0
            for i in range(quantidadeCores):
                c.setFont("Helvetica", 12)
                title = verificaGrade['descCor'][i]
                title = title[:9]

                # Posição vertical para cada linha
                y_position = (22.4 - 1.2 * i) * cm

                c.drawString(1.2 * cm, y_position, title)
                c.setLineWidth(1)
                c.line(1 * cm, y_position - 0.2 * cm, 20 * cm, y_position - 0.2 * cm)

                # Atualização do valor de inicioCores
                inicioCores = y_position
        else:
            quantidadeCores =0
            LimiteDaGrade(c, 10)



        # Inserir uma linha
        c.setLineWidth(1.3)  # Definir a largura da linha em 1 ponto
        c.line(0 * cm, 26.1 * cm, 27.8 * cm, 26.1 * cm)  # Desenhar uma linha









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

def BuscarCliente(codCliente):
    consulta = ClientesJohnField.ConsultaClientesEspecifico(codCliente)
    return consulta

def BucarOP(idOP):
    consulta = OP_JonhField.BuscandoOPEspecifica(idOP)
    return consulta

def OP_Tam_Cores(codOP, codCliente):
    consulta = OP_Tam_Cor_JohnField.ConsultaTamCor_OP(codOP, codCliente)
    return consulta

def LimiteDaGrade(c,limite):
    # Linha Horizontal
    c.setLineWidth(3)  # Definir a largura da linha em 1 ponto
    c.line(1 * cm, (limite+1.35) * cm, 20 * cm, (limite+1.35)  * cm)  # Desenhar a antipenultima linha de delimitacao
    # Definir os cabeçalhos
    c.setFont("Helvetica-Bold", 14)
    title = 'TOTAL'
    c.drawString(1.2 * cm, (limite+0.4) * cm, title)

    c.setLineWidth(2.5)  # Definir a largura da linha em 1 ponto
    c.line(1 * cm, limite * cm, 20 * cm, limite * cm)  # Desenhar a ultima linha de delimitacao

    # Linhas Vericais
    c.setLineWidth(2.5)  # Definir a largura da linha em 1 ponto
    c.line(1.0 * cm, limite * cm, 1.0 * cm, 24.5 * cm)  # Desenhar a primeira linha vertical da grade
    c.line(4.0 * cm, limite * cm, 4.0 * cm, 24.5 * cm)  # Desenhar a primeira linha vertical da grade
    c.line(20 * cm, limite * cm, 20 * cm, 24.5 * cm)  # Desenhar a segunda linha vertical da grade
    c.line(17.8 * cm, limite * cm, 17.8 * cm, 24.5 * cm)  # Desenhar a terceira linha vertical da grade