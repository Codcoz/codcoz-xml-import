from flask import Flask, request, jsonify
import os
import psycopg2
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from flask_cors import CORS

load_dotenv()

def get_conn():
    return psycopg2.connect(os.getenv("SQL_URL"))

conn = get_conn()
cursor = conn.cursor()

app = Flask(__name__)
CORS(app)

def extrair_dados_nfe(xml_content) -> dict:
    root = ET.fromstring(xml_content)
    ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

    infNFe = root.find('.//nfe:infNFe', ns)
    id_nfe = infNFe.get('Id') if infNFe is not None else None
    dhEmi = infNFe.findtext('.//nfe:ide/nfe:dhEmi', default='', namespaces=ns)

    produtos = []
    for det in root.findall('.//nfe:det', ns):
        prod = det.find('nfe:prod', ns)
        if prod is not None:
            dVal = None
            rastro = det.find('.//nfe:rastro', ns)
            if rastro is not None:
                dVal = rastro.findtext('nfe:dVal', default='', namespaces=ns)

            produtos.append({
                'nome_produto': prod.findtext('nfe:xProd', default='', namespaces=ns),
                'unidade_medida': prod.findtext('nfe:uCom', default='', namespaces=ns),
                'quantidade': prod.findtext('nfe:qCom', default='', namespaces=ns),
                'valor_unitario': prod.findtext('nfe:vUnCom', default='', namespaces=ns),
                'valor_total': prod.findtext('nfe:vProd', default='', namespaces=ns),
                'ean': prod.findtext('nfe:cEAN', default='', namespaces=ns),
                'data_validade': dVal
            })

    return {
        'id_nfe': id_nfe,
        'data_emissao': dhEmi,
        'produtos': produtos
    }

def normalize_unidade_medida(unidade_medida):
    # Tenta selecionar a unidade de medida baseado em sua sigla
    cursor.execute("SELECT id FROM unidade_medida WHERE sigla = UPPER(%s);", (unidade_medida, ))
    row = cursor.fetchone()

    # Se não existir, insere essa unidade de medida no banco
    if row:
        return row[0]
    else:
        cursor.execute("INSERT INTO unidade_medida (sigla) VALUES (UPPER(%s)) RETURNING id;", (unidade_medida, ))
        return cursor.fetchone()[0]    

def normalize_produto(nome_produto, quantidade, unidade_medida, codigo_ean, empresa_id, validade):
    # Tenta selecionar o produto baseado em seu nome e empresa
    cursor.execute("SELECT id FROM produto WHERE codigo_ean = %s AND empresa_id = %s;", (codigo_ean, empresa_id))
    row = cursor.fetchone()
    
    # Se não existir, insere esse produto no banco
    if row:
        update_produto = """
            UPDATE produto
               SET quantidade = quantidade + %s,
                   validade   = %s
             WHERE codigo_ean = %s AND empresa_id = %s;
        """
        cursor.execute(update_produto, (quantidade, validade, codigo_ean, empresa_id))
        return row[0]
    else:
        # Mapeia a unidade de medida
        unidade_medida_id = normalize_unidade_medida(unidade_medida)

        cursor.execute(
            "INSERT INTO produto (nome, quantidade, unidade_medida_id, empresa_id, codigo_ean, validade) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
            (nome_produto, quantidade, unidade_medida_id, empresa_id, codigo_ean, validade)
        )
        return cursor.fetchone()[0]

def inserir_itens_e_produtos(produtos: list, empresa_id: int, pedido_id: int):
    insert_item_pedido = """
        INSERT INTO item_pedido (produto_id, pedido_id, quantidade, preco_unitario)
             VALUES (%s, %s, %s, %s);    
    """

    for prod in produtos:
        unidade_medida = prod.get("unidade_medida")
        nome_produto = prod.get("nome_produto")
        quantidade = int(float(prod.get("quantidade")))
        valor_unitario = float(prod.get("valor_unitario"))
        codigo_ean = prod.get("ean")
        data_validade = datetime.fromisoformat(prod.get("data_validade")).date() if prod.get("data_validade") != None else None
        
        produto_id = normalize_produto(nome_produto, quantidade, unidade_medida, codigo_ean, empresa_id, data_validade)

        cursor.execute(insert_item_pedido, (produto_id, pedido_id, quantidade, valor_unitario, ))

def inserir_nota_fiscal(nota_fiscal: dict, empresa_id: int):
    insert_pedido = """
        INSERT INTO pedido (empresa_id, data_compra, cod_nota_fiscal)
             VALUES (%s, %s, %s)
          RETURNING id;
    """
    id_nfe = nota_fiscal.get("id_nfe")
    data_emissao = datetime.fromisoformat(nota_fiscal.get("data_emissao"))

    cursor.execute(insert_pedido, (empresa_id, data_emissao.date(), id_nfe))
    pedido_id = cursor.fetchone()[0]
    produtos = nota_fiscal.get("produtos")

    inserir_itens_e_produtos(produtos, empresa_id, pedido_id)

    conn.commit()
    conn.close()

def select_pedidos(empresa_id: int) -> list:
    pedido_query = f"""
        SELECT id
             , empresa_id
             , data_compra
             , data_previsao
             , data_recebimento
             , descricao
             , cod_nota_fiscal
          FROM pedido
         WHERE empresa_id = {empresa_id}
         ORDER BY data_compra DESC;
    """
    df = pd.read_sql_query(pedido_query, conn)

    pedidos_list = []
    for _, row in df.iterrows():
        pedido = {
            "id": row.get("id"),
            "empresa_id": row.get("empresa_id"),
            "data_compra": str(row.get("data_compra")),
            "data_previsao": str(row.get("data_previsao")),
            "data_recebimento": str(row.get("data_recebimento")),
            "descricao": row.get("descricao"),
            "cod_nota_fiscal": row.get("cod_nota_fiscal")
        }

        pedidos_list.append(pedido)

    return pedidos_list

def select_itens_pedido(pedido_id):
    item_pedido_query = f"""
        SELECT p.id as produto_id
             , p.nome as nome_produto
             , p.marca
             , ip.quantidade 
          FROM item_pedido ip 
          JOIN produto p ON p.id = ip.produto_id 
         WHERE ip.pedido_id = {pedido_id};
    """
    df = pd.read_sql_query(item_pedido_query, conn)

    itens_pedido_list = []
    for _, row in df.iterrows():
        item_pedido = {
            "produto_id": row.get("produto_id"),
            "nome_produto": row.get("nome_produto"),
            "marca": row.get("marca"),
            "quantidade": row.get("quantidade"),
        }

        itens_pedido_list.append(item_pedido)

    return itens_pedido_list 


@app.route("/read_pedidos/<empresa_id>", methods=["GET"])
def read_pedidos(empresa_id):
    pedidos_list = select_pedidos(empresa_id)

    return jsonify({"status": "ok", "pedidos": pedidos_list})

@app.route("/read_itens_pedido/<pedido_id>", methods=["GET"])
def read_itens_pedido(pedido_id):

    itens_pedido_list = select_itens_pedido(pedido_id)

    return jsonify({"status": "ok", "pedidos": itens_pedido_list})

@app.route("/read_xml", methods=["POST"])
def read_xml():
    """
    Endpoint para upload de um arquivo .xml contendo uma NF-e.
    Retorna um JSON com o ID da nota, data de emissão e os produtos.
    """

    if 'file' not in request.files:
        return jsonify({'erro': 'Nenhum arquivo enviado.'}), 400

    file = request.files['file']

    if not file.filename.lower().endswith('.xml'):
        return jsonify({'erro': 'O arquivo deve ter extensão .xml.'}), 400

    try:
        xml_content = file.read()
        dict_nfe = extrair_dados_nfe(xml_content)
        return jsonify(dict_nfe), 200
    except Exception as e:
        return jsonify({'erro': f'Erro ao processar XML: {str(e)}'}), 500   

@app.route("/insert_xml", methods=["POST"])
def insert_xml():
    """
    Endpoint para upload de um arquivo .xml contendo uma NF-e.
    Insere no banco de dados os produtos que foram identificados na NF-e.
    """

    empresa_id = request.form.get("empresa_id")

    if not empresa_id:
        return jsonify({"error": "ID da empresa não informado!"}), 400

    if 'file' not in request.files:
        return jsonify({'erro': 'Nenhum arquivo enviado.'}), 400

    file = request.files['file']

    if not file:
        return jsonify({'erro': 'Arquivo .xml não enviado.'}), 400

    if not file.filename.lower().endswith('.xml'):
        return jsonify({'erro': 'O arquivo deve ter extensão .xml.'}), 400

    try:
        xml_content = file.read()
        dict_nfe = extrair_dados_nfe(xml_content)
        inserir_nota_fiscal(dict_nfe, int(empresa_id))
        return jsonify({"ok": f"Nota fiscal importada!"}), 200
    except Exception as e:
        return jsonify({'erro': f'Erro ao processar XML: {str(e)}'}), 500   

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat()
    })

if __name__ == "__main__":
    app.run(debug=True)
