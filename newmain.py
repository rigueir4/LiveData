import pandas as pd
import sqlite3 as lite
import pathlib
import numpy as np

class dattta():
  newfile = ''
  newfile_path = ''
  
  produtos = ['nefc', 'fulive', 'pplay']
  dbstring = ''
  db = ''
  cur = ''
  table=''
  df_sql = ""
  df_csv = ""
  list_csv = ''
  list_sql = ''
  list_validated = ''

  def __init__(self, produto):
    if produto =='nefc': self.dbstring = 'nefc_db.db'    
    elif produto =='fulive' : self.dbstring = 'fulive_db.db'

    self.db = lite.connect(self.dbstring)
    self.cur = self.db.cursor()


    

  def data_entry(self, tipo):
    if tipo == 'uso': self.table='uso'
    elif tipo == 'assin' or tipo == 'assinaturas': self.table = 'assinaturas'
    elif tipo =='user'or tipo=='usuarios':self.table = 'users'


    db = self.db
    #pegando dataframe do sql
    self.df_sql = pd.read_sql(f'SELECT * FROM {self.table}', db)



    def rename_csv(tipo):
      if tipo == 'uso' or tipo=='usage':
        path = pathlib.Path('.')/'csv-inter/uso'
        for file in path.iterdir():
            if file.is_file():
                self.newfile = 'uso_csv' + file.suffix
                self.newfile_path = file.rename(path / self.newfile)
                return file.rename(path / self.newfile)
      elif tipo == 'assin' or tipo =='assinaturas':
        path = pathlib.Path('.')/'csv-inter/assinaturas'
        for file in path.iterdir():
            if file.is_file():
                self.newfile = 'assinatura_csv' + file.suffix
                self.newfile_path = file.rename(path / self.newfile)
                return file.rename(path / self.newfile)
      elif tipo == 'user' or tipo =='usuarios':
        path = pathlib.Path('.')/'csv-inter/user'
        for file in path.iterdir():
            if file.is_file():
                self.newfile = 'users_csv' + file.suffix
                self.newfile_path = file.rename(path / self.newfile)
                return file.rename(path / self.newfile)
    rename_csv(tipo)


    #pegando dataframe do csv
    self.df_csv = pd.DataFrame(pd.read_csv(self.newfile_path, sep =';'))
    self.list_csv = self.df_csv.values.tolist()
    self.list_sql = self.df_sql.values.tolist()
    self.list_validated = [x for x in self.list_csv if not x in self.list_sql]

    if tipo=='uso' or tipo=='usage':
      for item in self.list_validated :
        self.cur.execute("INSERT INTO uso(nome, email, categoria, conteudo, acessado_em) VALUES(?, ? ,? ,? ,?)", (item[0], item[1], item[2], item[3], item[4]))
      self.db.commit()
    elif tipo =='assin' or tipo =='assinaturas':
      for item in self.list_validated :
        self.cur.execute("INSERT INTO assinaturas(id, status, data, cancelamento, cliente, plano, coupon_code, valor, parcelas) VALUES(?, ? ,? ,? ,?, ?, ?, ?, ?)", (item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8]))
      self.db.commit()
    elif tipo =='user' or tipo =='usuarios':
      for item in self.list_validated :
        self.cur.execute("INSERT INTO users(canceladas, falhas, trial, ultimo_acesso, criado_em, data_inativacao, documento, telefone, celular, cep, endereco, numero, complemento, bairro, cidade, estado, operador, perfil_de_acesso, plano) VALUES(?, ? ,? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[10], item[11], item[12], item[13], item[14], item[15], item[16], item[17],item[18]))
      self.db.commit()

test = dattta('fulive')
test.data_entry('uso')

    