import pandas as pd
import os
import re

# Caminho onde estão localizados os arquivos CSV
caminho_diretorio = r'C:\Users\vinic\Downloads\nps'
nomes_arquivos = ['2024.csv', '2023.csv']

# Função para carregar CSV em pedaços e concatenar
def carregar_csv_em_pedacos(caminho_arquivo, chunk_size=10000):
    chunks = []
    try:
        for chunk in pd.read_csv(caminho_arquivo, delimiter=';', encoding='utf-8', on_bad_lines='skip', low_memory=False, chunksize=chunk_size):
            chunks.append(chunk)
    except pd.errors.ParserError as e:
        print(f"Erro ao ler o arquivo {caminho_arquivo}: {e}")
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

# Carregar os arquivos CSV em pedaços e concatenar
dataframes = [carregar_csv_em_pedacos(os.path.join(caminho_diretorio, nome_arquivo)) for nome_arquivo in nomes_arquivos]
df_unico = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

# Importar novo dataframe
df2 = carregar_csv_em_pedacos(os.path.join(caminho_diretorio, '2022.csv'))

# Verificação se df2 foi carregado corretamente
if df2.empty:
    raise ValueError("O arquivo 2022.csv não foi carregado corretamente.")

# Remover linhas com valores nulos na coluna 'nps'
df2.dropna(subset=['nps'], inplace=True)

# Converter 'nps' para inteiros
df2['nps'] = pd.to_numeric(df2['nps'], errors='coerce').astype('Int64')

# Remover colunas indesejadas do df2
colunas_para_remover = [
    'Nome', 'email', 'telefone', 'titulo', 'tipo', 'data_de_envio',
    'Gratuito', 'Preço', 'CodPessoaF', 'MacroEvento', 'NomeProduto', 'CodEvento', 'MacroEvento', 'TipoAtendimento', 'Setor',
    'data_envio', 'Resolvido', 'data_inicio_atendimento', 'TipoEmpreendimento', 'Atendente',
    'data_encerramento_atendimento', 'total_atendentes', 'Notas', 'Tags',
    'Categoria', 'Status do Atendimento', 'NomeEmpresa', 'CNPJ', 'Telefone',
    'Publico', 'Sexo', 'DataNascimento', 'CPF', 'TipoInstrumento',
    'SiglaSebrae', 'NomeIniciativa', 'nupratif', 'Vinculo', 'PrimeiroNome',
    'CodRealizacao', 'Tema', 'DataHoraInicioRealizacao', 'Atendente.1',
    'NomeFantasia', 'RazaoSocial', 'CodigoEmpreendimento', 'ContatoPorTelefone',
    'ContatoPorEmail', 'NomeTratamento', 'TipoRealizacao', 'NomeEvento',
    'DataHoraFimRealizacao', 'Protocolo', 'CodAtendente', 'Celular', 'NA NPS',
    'CodProduto', 'Titulo do Conteúdo', 'URL Atendimento', 'Situação',
    'Data de Finalização', 'Tipo de Instrutor', 'TipoVinculo', 'CodSebrae Atend',
    'DescGenero', 'Esta informação está correta?', 'Antes de ir, sinta-se à vontade de comentar o principal motivo da sua nota:  ?',
    'Você autoriza a publicação do seu comentário/elogio nas redes sociais do Sebrae?'
]

df2.drop(columns=colunas_para_remover, inplace=True, errors='ignore')
df2['codigo_cliente'] = df2['codigo_cliente'].astype(str).str.replace(r'\.0$', '', regex=True)

# Criar uma função para categorizar os clientes
def categorize_nps(score):
    if pd.isna(score):
        return 'Valor Ausente'
    elif score >= 0 and score <= 6:
        return 'Detrator'
    elif score >= 7 and score <= 8:
        return 'Neutro'
    elif score >= 9 and score <= 10:
        return 'Promotor'
    else:
        return 'Fora do Intervalo'

# Aplicar a função à coluna 'nps' e criar uma nova coluna 'categoria_nps'
df2['categoria_nps'] = df2['nps'].apply(categorize_nps)

# Remover linhas com valores nulos na coluna 'codigo_cliente'
df2.dropna(subset=['codigo_cliente'], inplace=True)

# Substituir os códigos de cliente com indicativo de ano pelos códigos corretos
def substituir_codigo_cliente(df):
    # Identificar os códigos corretos para cada cliente
    codigos_corretos = df[~df['codigo_cliente'].str.contains(r'\|\d{4}', regex=True)].drop_duplicates('PFNomeCliente')
    codigos_corretos_dict = pd.Series(codigos_corretos.codigo_cliente.values, index=codigos_corretos.PFNomeCliente).to_dict()
    
    # Substituir os códigos com indicativo de ano pelos códigos corretos
    df['codigo_cliente'] = df.apply(
        lambda row: codigos_corretos_dict.get(row['PFNomeCliente'], row['codigo_cliente']) if re.search(r'\|\d{4}', str(row['codigo_cliente'])) else row['codigo_cliente'],
        axis=1
    )
    return df

# Aplicar a função ao dataframe df_unico
df_unico = substituir_codigo_cliente(df_unico)

# Criar novas colunas no df2 para identificar a categoria dos clientes no df_unico
df2['e_promotor'] = df2['codigo_cliente'].isin(df_unico[df_unico['categoria_nps'] == 'Promotor']['codigo_cliente']).astype(int)
df2['e_neutro'] = df2['codigo_cliente'].isin(df_unico[df_unico['categoria_nps'] == 'Neutro']['codigo_cliente']).astype(int)
df2['e_detrator'] = df2['codigo_cliente'].isin(df_unico[df_unico['categoria_nps'] == 'Detrator']['codigo_cliente']).astype(int)

# Criar coluna indicando se o cliente voltou a ser atendido
df2['voltou_a_ser_atendido'] = df2['codigo_cliente'].isin(df_unico['codigo_cliente']).astype(int)

# Importar colunas adicionais do df_unico para df2
colunas_importar = ['Projeto', 'Acao', 'tema', 'subtema', 'instrumento', 'AtendimentoCanal']
for coluna in colunas_importar:
    df2[coluna + '2'] = df2['codigo_cliente'].map(df_unico.set_index('codigo_cliente')[coluna])

# Exportar o novo dataframe para um arquivo Excel
caminho_arquivo_excel = os.path.join(caminho_diretorio, 'df2_atualizado.xlsx')
df2.to_excel(caminho_arquivo_excel, index=False)

print(f"Arquivo Excel exportado com sucesso para {caminho_arquivo_excel}")
