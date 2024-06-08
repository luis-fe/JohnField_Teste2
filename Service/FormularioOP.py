import pandas as pd
from reportlab.lib.pagesizes import portrait  # Alteração aqui
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
import tempfile
from reportlab.graphics.barcode import code128
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
        title = 'ORDEM DE PRODUÇÃO'
        c.drawString(7.0 * cm, 28.7 * cm, title)

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
        c.drawString(0.3 * cm, 23.8 * cm, title)


        # Adicionando cabecalho de SUBTOTAIS
        c.setFont("Helvetica-Bold", 14)
        title = 'TOTAL'
        c.drawString(18.0 * cm, 23.8 * cm, title)



        # Título centralizado
        c.setFont("Helvetica-Bold", 11)
        title = 'Data Criação OP:'
        c.drawString(14.3 * cm, 25.0 * cm, title)

        # Título centralizado
        c.setFont("Helvetica-Bold", 10)
        title = 'Data e Hora Impressão:'
        c.drawString(12.3 * cm, 1.2 * cm, title)

        # Título centralizado
        c.setFont("Helvetica", 10)
        title = str(OP_JonhField.obterHoraAtual())
        c.drawString(16.5 * cm, 1.2 * cm, title)

        # Título centralizado
        c.setFont("Helvetica", 11)
        title = str(informacoes['dataCriacaoOP'][0])
        title = title[0:2]+'/'+title[3:5]+'/'+title[6:10]
        c.drawString(17.6 * cm, 25.0 * cm, title)

        c.setLineWidth(2.5)  # Definir a largura da linha em 1 ponto
        c.line(0.2 * cm, 24.5 * cm, 20 * cm, 24.5 * cm)  # Desenhar uma linha
        c.line(0.2 * cm, 23.5 * cm, 20 * cm, 23.5 * cm)  # Desenhar uma linha


        # DELIMITACAO DA TABELA DE GRADES
        #_______________________________________________________________________________________________________
        # Inserir uma linha vertical
        c.setLineWidth(1.0)  # Definir a largura da linha em 1 ponto

        verificaGrade = OP_Tam_Cores(codOP,codCliente)
        print(verificaGrade)

        if verificaGrade['status'][0] == True:
            totalQ = informacoes['quantidade'][0]
            totalQ = round(float(totalQ))


            quantidadeCores = verificaGrade['descCor'].count()

            # Etapa: Avaliando a quantidade de cores para construir os limites
            #######################################################################
            if quantidadeCores <= 10:
                LimiteDaGrade(c, 10,str(totalQ))
            else:
                LimiteDaGrade(c, 5.2,str(totalQ))
            # Fim Etapa
            #########################################################################

            inicioCores = 0
            for i in range(quantidadeCores):
                #iNSERIDO OS TAMANHOS
                posicaoTamanho = CabecalhosTamanhos(c, verificaGrade, i, quantidadeCores)

                #Inserindo a descricao das cores
                c.setFont("Helvetica", 12)
                title = verificaGrade['descCor'][i]
                title = title[:9]



                # Posição vertical para cada linha
                y_position = (22.4 - 1.2 * i) * cm

                c.drawString(0.35 * cm, y_position, title)
                c.setLineWidth(1)
                c.line(0.2 * cm, y_position - 0.2 * cm, 20 * cm, y_position - 0.2 * cm)

                qTotal, posicaoQuantidade = InserindoQuantidades(c,verificaGrade,y_position,i)

                title = str(qTotal)
                c.drawString(18 * cm, y_position, title)

                # Atualização do valor de inicioCores
                inicioCores = y_position


        else:
            quantidadeCores =0

            LimiteDaGrade(c, 10,'')



        # Inserir uma linha
        c.setLineWidth(1.3)  # Definir a largura da linha em 1 ponto
        c.line(0 * cm, 25.6 * cm, 27.8 * cm, 25.6 * cm)  # Desenhar uma linha









        # Inserir uma imagem
        imagem_path = "Logo.png"  # Substitua pelo caminho da sua imagem
        c.drawImage(imagem_path, 0.2 * cm, 26.8 * cm, width=3 * cm, height=2.2 * cm)  # Posição e dimensões da imagem


        qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
        qr.add_data(str(codOP)+'||'+str(codCliente))  # Substitua pelo link desejado
        qr.make(fit=True)

        barcode = code128.Code128(str(str(codOP)+'&'+str(codCliente)), barWidth=1.0, barHeight=23.9)  # Criar código de barras
        barcode.drawOn(c, 15. * cm, 25.8 * cm)  # Desenhar código de barras na posição desejada

        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
        c.drawImage(qr_filename, 18.0 * cm, 27.0 * cm, width=2.2 * cm, height= 2.20 * cm)



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

def LimiteDaGrade(c,limite,total):
    # Linha Horizontal
    c.setLineWidth(3)  # Definir a largura da linha em 1 ponto
    c.line(0.2 * cm, (limite+1.35) * cm, 20 * cm, (limite+1.35)  * cm)  # Desenhar a antipenultima linha de delimitacao
    # Definir os cabeçalhos
    c.setFont("Helvetica-Bold", 14)
    title = 'TOTAL'
    c.drawString(0.3 * cm, (limite+0.4) * cm, title)
    title = total
    c.drawString(18 * cm, (limite+0.4) * cm, title)

    c.setLineWidth(2.5)  # Definir a largura da linha em 1 ponto
    c.line(0.2 * cm, limite * cm, 20 * cm, limite * cm)  # Desenhar a ultima linha de delimitacao

    # Linhas Vericais
    c.setLineWidth(2.5)  # Definir a largura da linha em 1 ponto
    c.line(0.2 * cm, limite * cm, 0.2 * cm, 24.5 * cm)  # Desenhar a primeira linha vertical da grade
    c.line(4.0 * cm, limite * cm, 4.0 * cm, 24.5 * cm)  # Desenhar a primeira linha vertical da grade
    c.line(20 * cm, limite * cm, 20 * cm, 24.5 * cm)  # Desenhar a segunda linha vertical da grade
    c.line(17.8 * cm, limite * cm, 17.8 * cm, 24.5 * cm)  # Desenhar a terceira linha vertical da grade


def CabecalhosTamanhos(c, dataframe, i, quantidadeCores):
    tamanhos = dataframe['tamanho'][i]

    AvaliarTam = len(tamanhos)

    if AvaliarTam <= 9:
        posicaoTamanho = 4.2

        for tamanho in tamanhos:
            c.setFont("Helvetica", 12)
            title = tamanho[0:6]
            title = title.replace(' ', '')
            title = title.replace('(inf', 'inf')
            title = title.replace('(in', 'inf')
            c.drawString(posicaoTamanho * cm, 23.8 * cm, title)
            posicaoTamanho = posicaoTamanho + 1.5

            if quantidadeCores <= 10:
                c.setLineWidth(1.0)  # Definir a largura da linha em 1 ponto
                c.line((posicaoTamanho - 0.15) * cm, (23.8 + 0.7) * cm, (posicaoTamanho - 0.15) * cm, (11.4) * cm)
    else:
        posicaoTamanho = 4.2
        for tamanho in tamanhos:
            c.setFont("Helvetica", 10)
            title = tamanho[0:6]
            title = title.replace(' ', '')
            title = title.replace('(inf', 'inf')
            title = title.replace('(in', 'inf')


            c.drawString(posicaoTamanho * cm, 23.8 * cm, title)
            posicaoTamanho = posicaoTamanho + 0.92

            if quantidadeCores <= 10:
                c.setLineWidth(1.0)  # Definir a largura da linha em 1 ponto
                c.line((posicaoTamanho - 0.2) * cm, (23.8 + 0.7) * cm, (posicaoTamanho - 0.2) * cm, (11.4) * cm)

    return posicaoTamanho


def InserindoQuantidades(c,verificaGrade,y_position,i):
    quantidades = verificaGrade['quantidade'][i]
    qTotal = 0
    AvaliarTam = len(quantidades)

    if AvaliarTam <= 9:
        posicaoQuantidade = 4.4

        for q in quantidades:
            c.setFont("Helvetica", 12)
            title = str(q)
            c.drawString(posicaoQuantidade * cm, y_position, title)
            posicaoQuantidade = posicaoQuantidade + 1.5

            qTotal = qTotal + q
    else:
        posicaoQuantidade = 4.33
        for q in quantidades:
            c.setFont("Helvetica", 10)
            title = str(q)
            c.drawString(posicaoQuantidade * cm, y_position, title)
            posicaoQuantidade = posicaoQuantidade + 0.897

            qTotal = qTotal + q

    return qTotal, posicaoQuantidade