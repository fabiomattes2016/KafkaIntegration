import pandas as pd
import pyodbc


print("--- Selecionando arquivo CSV")
data = pd.read_csv(r'export_data.csv')

print("--- Tratando dados")
data['UNIDADE'].fillna(value="", inplace=True)
data['QUARTO_LEITO'].fillna(value="", inplace=True)
data['CONTA'].fillna(value=0, inplace=True)
data['PRONTUARIO'].fillna(value=0, inplace=True)
data['DATA_DE_INTERNACAO'].fillna(value='1970-01-01 01:00:00', inplace=True)
data['DATA_DE_ALTA_MEDICA'].fillna(value='1970-01-01 01:00:00', inplace=True)
data['DATA_ALTA_HOSPITALAR'].fillna(value='1970-01-01 01:00:00', inplace=True)
data['NOME_DO_PACIENTE'].fillna(value="", inplace=True)
data['SEXO'].fillna(value="", inplace=True)
data['ESTADO_CIVIL'].fillna(value="", inplace=True)
data['DATA_DE_NASCIMENTO'].fillna(value='1970-01-01 01:00:00', inplace=True)
data['NOME_PROFISSIONAL'].fillna(value=0, inplace=True)
data['DIAS'].fillna(value=0, inplace=True)
data['CONVENIO'].fillna(value="", inplace=True)
data['PLANO'].fillna(value="", inplace=True)
data['ESPECIALIDADE'].fillna(value="", inplace=True)
data['PROCEDENCIA'].fillna(value="", inplace=True)
data['NATUREZA_ATENDIMENTO'].fillna(value="", inplace=True)
data['TIPO_ATENDIMENTO'].fillna(value="", inplace=True)
data['TIPO_ALTA_HOSPITALAR'].fillna(value="", inplace=True)
data['CID_ENTRADA'].fillna(value="", inplace=True)
data['DIAGNOSTICO_ENTRADA'].fillna(value="", inplace=True)
data['CID_SAIDA'].fillna(value="", inplace=True)
data['DIAGNOSTICO_SAIDA'].fillna(value="", inplace=True)
data['MUNICIPIO'].fillna(value="", inplace=True)
data['IDADE'].fillna(value=0, inplace=True)
data['QUANTIDADE'].fillna(value="", inplace=True)

df = pd.DataFrame(data, columns=[
    'UNIDADE', 
    'QUARTO_LEITO', 
    'CONTA',
    'PRONTUARIO',
    'DATA_DE_INTERNACAO',
    'DATA_DE_ALTA_MEDICA',
    'DATA_ALTA_HOSPITALAR',
    'NOME_DO_PACIENTE',
    'SEXO',
    'ESTADO_CIVIL',
    'DATA_DE_NASCIMENTO',
    'NOME_PROFISSIONAL',
    'DIAS',
    'CONVENIO',
    'PLANO',
    'ESPECIALIDADE',
    'PROCEDENCIA',
    'NATUREZA_ATENDIMENTO',
    'TIPO_ATENDIMENTO',
    'TIPO_ALTA_HOSPITALAR',
    'CID_ENTRADA',
    'DIAGNOSTICO_ENTRADA',
    'CID_SAIDA',
    'DIAGNOSTICO_SAIDA',
    'MUNICIPIO',
    'IDADE',
    'QUANTIDADE'
])

print(df)

print("--- Criando a conexão com o SQL Server")
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=localhost;DATABASE=INDICADORES;UID=sa;PWD=Fabio1982Mattes')

cursor = conn.cursor()

print("--- Excluindo dados antigos")
cursor.execute(
    '''
    DROP TABLE IF EXISTS [dbo].[INTERNACOES] 
    '''
)

print("--- Recriando tabelas com dados atuais")
cursor.execute(
    '''
    CREATE TABLE [dbo].[INTERNACOES](
        [ID] [int] IDENTITY(1,1) NOT NULL,
        [UNIDADE] [varchar](120) NULL,
        [QUARTO_LEITO] [varchar](61) NULL,
        [CONTA] [int] NULL,
        [PRONTUARIO] [int] NULL,
        [DATA_DE_INTERNACAO] [datetime] NULL,
        [DATA_DE_ALTA_MEDICA] [datetime] NULL,
        [DATA_ALTA_HOSPITALAR] [datetime] NULL,
        [NOME_DO_PACIENTE] [varchar](60) NULL,
        [SEXO] [varchar](20) NULL,
        [ESTADO_CIVIL] [varchar](20) NULL,
        [DATA_DE_NASCIMENTO] [datetime] NULL,
        [NOME_PROFISSIONAL] [varchar](60) NULL,
        [DIAS] [int] NULL,
        [CONVENIO] [varchar](120) NULL,
        [PLANO] [varchar](60) NULL,
        [ESPECIALIDADE] [varchar](255) NULL,
        [PROCEDENCIA] [varchar](50) NULL,
        [NATUREZA_ATENDIMENTO] [varchar](255) NULL,
        [TIPO_ATENDIMENTO] [varchar](30) NULL,
        [TIPO_ALTA_HOSPITALAR] [varchar](255) NULL,
        [CID_ENTRADA] [varchar](30) NULL,
        [DIAGNOSTICO_ENTRADA] [varchar](255) NULL,
        [CID_SAIDA] [varchar](30) NULL,
        [DIAGNOSTICO_SAIDA] [varchar](255) NULL,
        [MUNICIPIO] [varchar](50) NULL,
        [IDADE] [int] NULL,
        [QUANTIDADE] [int] NULL
    ) ON [PRIMARY]
    '''
)

print("--- Inserindo dados atuais")
for row in df.itertuples():
    cursor.execute(
        '''
        INSERT INTO [dbo].[INTERNACOES]
           (
                [UNIDADE],
                [QUARTO_LEITO],
                [CONTA],
                [PRONTUARIO],
                [DATA_DE_INTERNACAO],
                [DATA_DE_ALTA_MEDICA],
                [DATA_ALTA_HOSPITALAR],
                [NOME_DO_PACIENTE],
                [SEXO],
                [ESTADO_CIVIL],
                [DATA_DE_NASCIMENTO],
                [NOME_PROFISSIONAL],
                [DIAS],
                [CONVENIO],
                [PLANO],
                [ESPECIALIDADE],
                [PROCEDENCIA],
                [NATUREZA_ATENDIMENTO],
                [TIPO_ATENDIMENTO],
                [TIPO_ALTA_HOSPITALAR],
                [CID_ENTRADA],
                [DIAGNOSTICO_ENTRADA],
                [CID_SAIDA],
                [DIAGNOSTICO_SAIDA],
                [MUNICIPIO],
                [IDADE],
                [QUANTIDADE]
            )
        VALUES
           (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''',
        row.UNIDADE,
        row.QUARTO_LEITO,
        row.CONTA,
        row.PRONTUARIO,
        row.DATA_DE_INTERNACAO,
        row.DATA_DE_ALTA_MEDICA,
        row.DATA_ALTA_HOSPITALAR,
        row.NOME_DO_PACIENTE,
        row.SEXO,
        row.ESTADO_CIVIL,
        row.DATA_DE_NASCIMENTO,
        row.NOME_PROFISSIONAL,
        row.DIAS,
        row.CONVENIO,
        row.PLANO,
        row.ESPECIALIDADE,
        row.PROCEDENCIA,
        row.NATUREZA_ATENDIMENTO,
        row.TIPO_ATENDIMENTO,
        row.TIPO_ALTA_HOSPITALAR,
        row.CID_ENTRADA,
        row.DIAGNOSTICO_ENTRADA,
        row.CID_SAIDA,
        row.DIAGNOSTICO_SAIDA,
        row.MUNICIPIO,
        row.IDADE,
        row.QUANTIDADE
    )

print("--- Finalizando")
conn.commit()

print("--- Finalizado com sucesso")
