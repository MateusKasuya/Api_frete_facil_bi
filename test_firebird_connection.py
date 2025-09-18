#!/usr/bin/env python3
"""
Arquivo de teste para conexão com Firebird
Testando a função get_firebird_connection do módulo conexaofb.py
"""

import sys
import os

# Adiciona o diretório app ao path para importar os módulos
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from db.conexaofb import get_firebird_connection

def test_firebird_connection():
    """Testa a conexão com o banco Firebird"""
    
    # Parâmetros de conexão extraídos do JDBC URL
    HOST = "localhost"
    PORT = 3050
    DATABASE = r"C:\Users\MateusKasuya\Documents\softcenter\elt\firebird_to_postgres\data\RCR.FDB"
    
    print("=" * 60)
    print("TESTE DE CONEXÃO FIREBIRD")
    print("=" * 60)
    print(f"Host: {HOST}")
    print(f"Port: {PORT}")
    print(f"Database: {DATABASE}")
    print("-" * 60)
    
    try:
        # Tenta estabelecer conexão
        print("Tentando conectar ao Firebird...")
        connection = get_firebird_connection(HOST, PORT, DATABASE)
        
        if connection:
            print("✅ Conexão estabelecida com sucesso!")
            
            # Testa uma consulta simples
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT 1 FROM RDB$DATABASE")
                result = cursor.fetchone()
                print(f"✅ Teste de consulta: {result}")
                
                # Mostra informações do banco
                cursor.execute("SELECT RDB$CHARACTER_SET_NAME FROM RDB$DATABASE")
                charset = cursor.fetchone()
                print(f"✅ Charset do banco: {charset[0] if charset else 'N/A'}")
                
                cursor.close()
                
            except Exception as query_error:
                print(f"❌ Erro ao executar consulta: {query_error}")
            
            # Fecha a conexão
            connection.close()
            print("✅ Conexão fechada com sucesso!")
            
        else:
            print("❌ Falha ao estabelecer conexão!")
            
    except Exception as e:
        print(f"❌ Erro de conexão: {e}")
        print(f"Tipo do erro: {type(e).__name__}")
        
        # Dicas de troubleshooting
        print("\n" + "=" * 60)
        print("DICAS DE TROUBLESHOOTING:")
        print("=" * 60)
        print("1. Verifique se o Firebird está rodando")
        print("2. Confirme se o arquivo do banco existe no caminho especificado")
        print("3. Verifique as credenciais (usuário e senha)")
        print("4. Confirme se a porta 3050 está aberta")
        print("5. Instale o driver: pip install firebird-driver")

def test_database_file_exists():
    """Verifica se o arquivo do banco existe"""
    database_path = r"C:\Users\MateusKasuya\Documents\softcenter\elt\firebird_to_postgres\data\RCR.FDB"
    
    print("\n" + "=" * 60)
    print("VERIFICAÇÃO DO ARQUIVO DO BANCO")
    print("=" * 60)
    print(f"Caminho: {database_path}")
    
    if os.path.exists(database_path):
        file_size = os.path.getsize(database_path)
        print(f"✅ Arquivo encontrado! Tamanho: {file_size:,} bytes")
    else:
        print("❌ Arquivo não encontrado!")
        
        # Verifica se o diretório existe
        directory = os.path.dirname(database_path)
        if os.path.exists(directory):
            print(f"📁 Diretório existe: {directory}")
            print("📋 Arquivos no diretório:")
            try:
                files = os.listdir(directory)
                for file in files:
                    if file.lower().endswith(('.fdb', '.gdb')):
                        print(f"  🗄️  {file}")
            except Exception as e:
                print(f"❌ Erro ao listar diretório: {e}")
        else:
            print(f"❌ Diretório não existe: {directory}")

if __name__ == "__main__":
    print("INICIANDO TESTES DE CONEXÃO FIREBIRD")
    print("=" * 60)
    
    # Primeiro verifica se o arquivo existe
    test_database_file_exists()
    
    # Depois testa a conexão
    test_firebird_connection()
    
    print("\n" + "=" * 60)
    print("TESTES CONCLUÍDOS")
    print("=" * 60)
