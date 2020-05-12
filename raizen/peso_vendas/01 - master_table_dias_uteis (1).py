import pandas as pd
import numpy as np
from datetime import date

import warnings
warnings.filterwarnings("ignore")

# !pip install pyarrow


def carnaval_pascoa(until_year):
    '''
    Função para cálculo das Datas do Carnaval e da Páscoa
    
    OBS: O Carnaval será a subtração de 47 dias da data da Páscoa
    fonte: https://www.vivaolinux.com.br/script/Calcular-a-data-do-Carnaval-e-da-Pascoa
    '''
    
    #Criando df vazio no formato padrão
    df_pascoa_carnaval = pd.DataFrame(columns=['Tipo', 'Dia', 'Mes', 'Data'])
    
    years_list = list(range(2012, until_year+1, 1))
    for ANO in years_list:
        X = 24
        Y = 5
        
        A = ANO % 19
        B = ANO % 4
        C = ANO % 7 
        D = ( 19 * A + X ) % 30
        E = ( 2 * B + 4 * C + 6 * D + Y ) % 7

        if ( (D + E ) > 9 ):
            DIA = D + E - 9
#             print ("A Pascoa de "+str(ANO)+" sera na data "+str(DIA)+" de Abril")
            MES = 4
            PASCOA = date(ANO, MES, DIA)
            CARNAVAL = date.fromordinal(PASCOA.toordinal()-47 )
#             print ("O Carnaval de "+str(ANO)+" sera na data :"+str(CARNAVAL))
        else:
            DIA = D + E + 22
#             print ("A Pascoa sera dia "+str(DIA)+" de Março")
            MES = 3
            PASCOA = date(ANO, MES, DIA)            
            CARNAVAL = date.fromordinal(PASCOA.toordinal()-47 )
#             print ("O Carnaval sera na data :"+str(CARNAVAL))
        
        carnaval = pd.to_datetime(str(CARNAVAL), format='%Y-%m-%d')
        df_pascoa_carnaval = df_pascoa_carnaval.append({'Tipo': 'N', 'Dia': carnaval.day, 'Mes': carnaval.month, 
                                                        'Data': carnaval},ignore_index=True)
        
        pascoa = pd.to_datetime(str(PASCOA), format='%Y-%m-%d')
        df_pascoa_carnaval = df_pascoa_carnaval.append({'Tipo': 'N', 'Dia': pascoa.day, 'Mes': pascoa.month, 
                                                        'Data': pascoa}, ignore_index=True)
        
    
    return df_pascoa_carnaval
    
    
# # Lendo Dados

# Lista de cidades do Brasil
lista_cidades = pd.read_excel('lista_cidades.xlsx', dtype=({'COD_CIDADE': str})) # , usecols=['UF', 'COD_CIDADE', 'CIDADE']

# Calendario (2012-2019)
calendario=pd.read_excel('calendario.xlsx')
calendario.Data = calendario.Data.astype(str)

#Criando datas de 2020 até 2030
calendario_2020_2030 = pd.DataFrame({'Data': pd.date_range('2020-01-01', '2029-12-31'), 'chave': 'key'})

#Concatenando
calendario = pd.concat([calendario, calendario_2020_2030])
calendario.Data = pd.to_datetime(calendario.Data.astype(str), format='%Y-%m-%d')

#Criando colunas de ano, mes e dia
calendario['Ano'] = calendario.Data.dt.year
calendario['Mes'] = calendario.Data.dt.month
calendario['Dia'] = calendario.Data.dt.day


# Feriados (2012-2019)
feriados=pd.read_excel('depara_feriados.xlsx', dtypes=({'Código IBGE\ndo Município': str}))

feriados_aux = feriados.copy()
feriados_aux = feriados_aux[['Tipo', 'UF', 'Código IBGE\ndo Município', 'Município', 'Dia', 'Mês', 'Ano', 'Data_feriados']]
feriados_aux.columns = ['Tipo', 'UF', 'COD_CIDADE', 'CIDADE', 'Dia', 'Mes', 'Ano', 'Data']

#Criando DataFrame para cada tipo de feriado (Nacional, Estadual e Municipal)
feriados_nac = feriados_aux[feriados_aux.Tipo == 'N'][['Tipo', 'Dia', 'Mes']].drop_duplicates()
feriados_est = feriados_aux[(feriados_aux.Tipo == 'E') & (feriados_aux.Ano == 2019)][['Tipo', 'UF', 'Dia', 'Mes', 'Data']].drop_duplicates()
feriados_mun = feriados_aux[(feriados_aux.Tipo == 'M') & (feriados_aux.Ano == 2019)][['Tipo', 'UF', 'CIDADE', 'Dia', 'Mes', 'Data']].drop_duplicates()


# # Criando chaves para os feriados

# Estaduais
feriados_est['chave_est'] = 0
feriados_est['chave_est'] = feriados_est.sort_values(by=['Data']).groupby(['UF']).cumcount().apply(lambda x: f'_{x+1}')
feriados_est['chave_est'] = feriados_est.UF + feriados_est.chave_est
feriados_est.drop(['Data'], axis=1, inplace=True)

# Municipais
feriados_mun['chave_mun'] = 0
feriados_mun['chave_mun'] = feriados_mun.sort_values(by=['Data']).groupby(['UF', 'CIDADE']).cumcount().apply(lambda x: f'_{x+1}')
feriados_mun['chave_mun'] = feriados_mun.CIDADE + '_' + feriados_mun.UF + feriados_mun.chave_mun
feriados_mun.drop(['Data'], axis=1, inplace=True)

#Criando df com datas da Pascoa e carnaval
df_pascoa_carnaval = carnaval_pascoa(2029)


# # # Merge dos bases

# Combinatorio Cidade x Data (Calendario completo)
calendario_combinado = lista_cidades.merge(calendario, on='chave', how='outer')


# # Combinatorio Cidade X Data e Feriados Nacionais
df_nac = calendario_combinado.merge(feriados_nac, on=['Dia', 'Mes'], how='outer')

#Removendo valores duplicados
df_nac.drop_duplicates(inplace=True)

#Duplicando as datas do dia 21-04-2019 (Feriado de Pascoa e Tiradentes no mesmo dia)
df_nac_dupli = df_nac.query("Data == '2019-04-21'")

#Concatenando com o DataFrame original
df_nac = pd.concat([df_nac, df_nac_dupli])

#Combinatorio feriados nacionais com a Pascoa e Carnaval
df_nac = df_nac.merge(df_pascoa_carnaval, on=['Data', 'Dia', 'Mes'], how='outer')

#Preenchendo o tipo de feriado
df_nac['Nac'] = np.where((df_nac.Tipo_x == 'N') | (df_nac.Tipo_y == 'N'), 1, 0)

#Formatando base
df_nac.drop(['Tipo_x', 'Tipo_y', 'chave'], axis=1, inplace=True)
df_nac.Ano = df_nac.Ano.astype(int).astype(str)
df_nac.Mes = df_nac.Mes.astype(int).astype(str)
df_nac.Dia = df_nac.Dia.astype(int).astype(str)

# Criando chave para os feriados nacionais
df_nac['chave_nac'] = 0
df_nac['chave_nac'] = df_nac.query("Nac == 1").sort_values(by=['Data']).groupby(['Ano', 'COD_CIDADE']).cumcount().apply(lambda x: f'br_{x+1}')


# # Combinatorio Cidade X Data e Feriados Estaduais
feriados_est.Mes = feriados_est.Mes.astype(str)
feriados_est.Dia = feriados_est.Dia.astype(str)
df_est = df_nac.merge(feriados_est, on=['Dia', 'Mes', 'UF'], how='outer')
df_est['Est'] = np.where(df_est.Tipo == 'E', 1, 0)
df_est.drop(['Tipo'], axis=1, inplace=True)


# # Combinatorio Cidade X Data e Feriados Municipais
feriados_mun.Mes = feriados_mun.Mes.astype(str)
feriados_mun.Dia = feriados_mun.Dia.astype(str)
df_mun = df_est.merge(feriados_mun, on=['Dia', 'Mes', 'UF', 'CIDADE'], how='outer')
df_mun['Mun'] = np.where(df_mun.Tipo == 'M', 1, 0)
df_mun.drop(['Tipo'], axis=1, inplace=True)

#Unificando coluna com as chaves de cada feriado
df_mun['chave'] = '0'
df_mun['chave'] = np.where(df_mun.Nac == 1.0, df_mun.chave_nac, df_mun.chave)
df_mun['chave'] = np.where((df_mun.Est == 1.0) & (df_mun.chave == '0'), df_mun.chave_est, df_mun.chave)
df_mun['chave'] = np.where((df_mun.Mun == 1.0) & (df_mun.chave == '0'), df_mun.chave_mun, df_mun.chave)

#Removendo colunas que nao interessam e valores nulos
df_mun.drop(['chave_nac', 'chave_est', 'chave_mun'], axis=1, inplace=True)
df_mun.dropna(inplace=True)


# # DataFrame Final
df_all = df_mun.copy()

#Formatando valores
df_all.Nac = df_all.Nac.astype(int).astype(str)
df_all.Est = df_all.Est.astype(int).astype(str)
df_all.Mun = df_all.Mun.astype(int).astype(str)

#Dia util: Segunda a Sabado (0 a 5) e sem feriado (Nacional, Estadual ou Municipal)
df_all['dia_util'] = np.where((df_all.Nac == '0') & (df_all.Est == '0') & (df_all.Mun == '0') & (df_all.Data.dt.weekday < 6),
                              1,
                              0)
                              
# Exportando para csv
df_all.to_csv('dias_uteis_cidade_chave (2012-2030).csv', encoding='utf-8', index=False)

# Exportando para parquet
df_all.to_parquet('dias_uteis_chave.parquet', index=False, engine='pyarrow')