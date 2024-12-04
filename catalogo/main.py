from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import (
    create_engine, Table, MetaData, select, update, insert, delete, 
    Column, String, Integer, Text, ForeignKey
)
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import sessionmaker, relationship
import boto3

# Configuração do SQLAlchemy
DATABASE_URL = "mysql+mysqlconnector://catalogo_user:Mudar123!@db-servicos:3306/catalogo"
SQS_UPDATE_QUEUE = "produto-atualizacao"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Criar tabela de categoria
categoria = Table(
    'categoria', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('nome', String(255), nullable=False)
)

# Atualizar tabela de produto para incluir categoria_id
produto = Table(
    'produto', metadata,
    Column('sku', String(255), primary_key=True),
    Column('nome', String(255)),
    Column('descr', Text),
    Column('urlImagem', String(255)),
    Column('categoria_id', Integer, ForeignKey('categoria.id'), nullable=True)
)

metadata.create_all(engine)

# Gerenciamento de sessão
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Inicializar cliente SQS
sqs = boto3.client('sqs',
                   endpoint_url='http://localstack:4566',
                   region_name='us-east-1',
                   aws_access_key_id='LKIAQAAAAAAAKWL2ASHI',
                   aws_secret_access_key='n327Qep6xt5SkDnWV8Lf7Ywb5U6C1B1rwtzoojba')

def send_to_sqs(queue_name: str, message_body: str):
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        sqs.send_message(QueueUrl=queue_url, MessageBody=message_body)
    except Exception as e:
        print(f"Error sending message to SQS: {e}")

app = FastAPI()

class Categoria(BaseModel):
    id: int = None
    nome: str

class Produto(BaseModel):
    sku: str
    nome: str
    descr: str
    urlImagem: str
    categoria_id: int = None

# CRUD para Categoria
@app.post("/categoria")
async def criar_categoria(nova_categoria: Categoria):
    session = SessionLocal()
    try:
        insert_query = insert(categoria).values(nome=nova_categoria.nome)
        result = session.execute(insert_query)
        session.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=400, detail="Erro ao registrar categoria")
        return {"message": "Categoria criada com sucesso"}
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=f"Erro ao registrar categoria: {e}")
    finally:
        session.close()

@app.get("/categoria/{id}")
async def obter_categoria(id: int):
    query = select(categoria).where(categoria.columns.id == id)
    with engine.connect() as connection:
        try:
            result = connection.execute(query).fetchone()
            if result is None:
                raise HTTPException(status_code=404, detail="Categoria não encontrada")
            column_names = [column.name for column in categoria.c]
            result_dict = dict(zip(column_names, result))
            return Categoria(**result_dict)
        except NoResultFound:
            raise HTTPException(status_code=404, detail="Categoria não encontrada")

@app.put("/categoria/{id}")
async def atualizar_categoria(id: int, atualiza_categoria: Categoria):
    session = SessionLocal()
    try:
        update_query = update(categoria).where(categoria.columns.id == id).values(nome=atualiza_categoria.nome)
        result = session.execute(update_query)
        session.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=400, detail="Erro ao atualizar categoria")
        return {"message": "Categoria atualizada com sucesso"}
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=f"Erro ao atualizar categoria: {e}")
    finally:
        session.close()

@app.delete("/categoria/{id}")
async def deletar_categoria(id: int):
    session = SessionLocal()
    try:
        delete_query = delete(categoria).where(categoria.columns.id == id)
        result = session.execute(delete_query)
        session.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=400, detail="Erro ao deletar categoria")
        return {"message": "Categoria deletada com sucesso"}
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=f"Erro ao deletar categoria: {e}")
    finally:
        session.close()

# Ajustes nos endpoints de Produto
@app.post("/produto")
async def criar_produto(novo_produto: Produto):
    session = SessionLocal()
    try:
        insert_query = insert(produto).values(
            sku=novo_produto.sku,
            nome=novo_produto.nome,
            descr=novo_produto.descr,
            urlImagem=novo_produto.urlImagem,
            categoria_id=novo_produto.categoria_id
        )
        result = session.execute(insert_query)
        session.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=400, detail="Erro ao registrar produto")
        send_to_sqs(SQS_UPDATE_QUEUE, novo_produto.json())
        return {"message": "Produto criado com sucesso"}
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=f"Erro ao registrar produto: {e}")
    finally:
        session.close()

@app.put("/produto/{sku}")
async def atualizar_produto(sku: str, atualiza_produto: Produto):
    session = SessionLocal()
    try:
        update_query = update(produto).where(produto.columns.sku == sku).values(
            nome=atualiza_produto.nome,
            descr=atualiza_produto.descr,
            urlImagem=atualiza_produto.urlImagem,
            categoria_id=atualiza_produto.categoria_id
        )
        result = session.execute(update_query)
        session.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=400, detail="Erro ao atualizar produto")
        send_to_sqs(SQS_UPDATE_QUEUE, atualiza_produto.json())
        return {"message": "Produto atualizado com sucesso"}
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=f"Erro ao atualizar produto: {e}")
    finally:
        session.close()
