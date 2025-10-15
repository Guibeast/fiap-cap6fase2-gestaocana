import oracledb
from datetime import datetime
import os
import pandas as pd

DB_USER = "RM562211"
DB_PASSWORD = "031099"
DB_DSN = "oracle.fiap.com.br:1521/ORCL"

# --- Constante para o preço da tonelada  ---
PRECO_POR_TONELADA = 150.0


def conectar_oracle():
    """Tenta conectar ao banco de dados Oracle e retorna um objeto de conexão."""
    try:
        conexao = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)
        return conexao
    except Exception as e:
        print(f"Erro ao conectar ao Oracle: {e}")
        print("Verifique se as suas credenciais (usuário, senha e DSN) estão corretas.")
        return None


# --- FUNÇÕES DE VALIDAÇÃO ---
def obter_data_valida(prompt):
    """Solicita uma data ao usuário e continua pedindo até que um formato válido seja inserido."""
    while True:
        try:
            data_str = input(prompt)
            data_valida = datetime.strptime(data_str, '%d/%m/%Y').date()
            return data_valida
        except ValueError:
            print("Formato de data inválido. Por favor, use DD/MM/AAAA (ex: 25/12/2024).")


def obter_numero_valido(prompt, tipo=float):
    """
    Solicita um número ao usuário e continua pedindo até que uma entrada válida e positiva seja fornecida.
    """
    while True:
        try:
            valor = tipo(input(prompt).replace(',', '.'))
            if valor < 0:
                print("Erro: O valor não pode ser negativo. Tente novamente.")
                continue
            return valor
        except ValueError:
            if tipo == int:
                print("Erro: Entrada inválida. Por favor, digite um número inteiro (ex: 10).")
            else:
                print("Erro: Entrada inválida. Por favor, digite um número (ex: 80 ou 80.5).")


# --- FUNÇÕES PRINCIPAIS ---
def registrar_colheita(conexao):
    """Procedimento para registrar uma nova colheita no banco de dados"""
    print("\n--- Registrar Nova Colheita ---")
    try:
        data_colheita = obter_data_valida("Data da Colheita (DD/MM/AAAA): ")
        area_hectares = obter_numero_valido("Área colhida (hectares): ")
        producao_toneladas = obter_numero_valido("Produção total (toneladas): ")
        perda_percentual = obter_numero_valido("Perda estimada na colheita (%): ")

        dados_colheita = {
            "data": data_colheita,
            "area": area_hectares,
            "producao": producao_toneladas,
            "perda": perda_percentual
        }

        with conexao.cursor() as cursor:
            sql = """INSERT INTO COLHEITAS_CANA 
                     (DATA_COLHEITA, AREA_HECTARES, PRODUCAO_TONELADAS, PERDA_PERCENTUAL) 
                     VALUES (:data, :area, :producao, :perda)"""
            cursor.execute(sql, dados_colheita)

        conexao.commit()
        print("\nColheita registrada com sucesso no banco de dados!")

    except Exception as e:
        print(f"\nOcorreu um erro inesperado ao registrar a colheita: {e}")
        conexao.rollback()

def exibir_historico(conexao):
    """Função para buscar, processar e exibir o histórico de colheitas usando Pandas."""
    print("\n--- Histórico de Colheitas ---")
    try:

        sql_query = """
            SELECT ID_COLHEITA, DATA_COLHEITA, AREA_HECTARES, PRODUCAO_TONELADAS, PERDA_PERCENTUAL 
            FROM COLHEITAS_CANA 
            ORDER BY ID_COLHEITA
        """

        df = pd.read_sql(sql_query, conexao)

        if df.empty:
            print("Nenhuma colheita registrada ainda.")
            return False

        # Renomeando colunas para uma melhor apresentação
        df.rename(columns={
            'ID_COLHEITA': 'ID',
            'DATA_COLHEITA': 'Data',
            'AREA_HECTARES': 'Área (ha)',
            'PRODUCAO_TONELADAS': 'Produção (t)',
            'PERDA_PERCENTUAL': 'Perda (%)'
        }, inplace=True)

        # Calculando o prejuízo diretamente no DataFrame
        df['Prejuízo (R$)'] = (df['Produção (t)'] * (df['Perda (%)'] / 100)) * PRECO_POR_TONELADA

        # Formatando a coluna de data para o padrão brasileiro
        df['Data'] = pd.to_datetime(df['Data']).dt.strftime('%d/%m/%Y')

        # O Pandas formata e imprime a tabela de forma organizada
        print(df.to_string(index=False))

        # Calculando e imprimindo o total
        total_prejuizo = df['Prejuízo (R$)'].sum()
        print("\n" + "-" * 40)
        print(f"PREJUÍZO TOTAL ESTIMADO: R$ {total_prejuizo:.2f}")
        print("-" * 40)

        return True

    except Exception as e:
        print(f"Erro ao buscar histórico: {e}")
        return False


def apagar_registro(conexao):
    """Procedimento para apagar um registro de colheita do banco de dados."""
    print("\n--- Apagar Registro de Colheita ---")
    if not exibir_historico(conexao):
        return

    try:
        id_para_apagar = obter_numero_valido("\nDigite o ID do registro que deseja apagar: ", tipo=int)

        confirmacao = input(
            f"Tem certeza que deseja apagar o registro com ID {id_para_apagar}? (S/N): ").strip().upper()

        if confirmacao == 'S':
            with conexao.cursor() as cursor:
                sql = "DELETE FROM COLHEITAS_CANA WHERE ID_COLHEITA = :id"
                cursor.execute(sql, [id_para_apagar])

                if cursor.rowcount == 0:
                    print(f"\nNenhum registro encontrado com o ID {id_para_apagar}.")
                else:
                    conexao.commit()
                    print(f"\nRegistro com ID {id_para_apagar} apagado com sucesso.")
        else:
            print("\nOperação cancelada.")

    except Exception as e:
        print(f"\nOcorreu um erro ao tentar apagar o registro: {e}")
        conexao.rollback()


def menu():
    """Função principal que gerencia o menu e a interação com o usuário."""
    conexao = conectar_oracle()
    if not conexao:
        input("\nPressione Enter para sair...")
        return

    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n===== AGROTECH FIAP - GESTÃO DE COLHEITA DE CANA =====")
        print("1. Registrar nova colheita")
        print("2. Exibir histórico de colheitas e prejuízo")
        print("3. Apagar um registro de colheita")
        print("4. Sair")

        escolha = input("Escolha uma opção: ")

        if escolha == '1':
            registrar_colheita(conexao)
        elif escolha == '2':
            exibir_historico(conexao)
        elif escolha == '3':
            apagar_registro(conexao)
        elif escolha == '4':
            break
        else:
            print("Opção inválida. Tente novamente.")

        input("\nPressione Enter para continuar...")

    if conexao:
        conexao.close()
        print("\nConexão com o Oracle fechada. Programa finalizado.")


if __name__ == "__main__":
    menu()

