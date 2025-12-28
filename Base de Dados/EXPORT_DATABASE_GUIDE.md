# Guia: Como Exportar a Base de Dados para o GitHub

Este documento explica como exportar a base de dados do SQL Server para um ficheiro `.sql` que pode ser partilhado via GitHub.

## Por que usar scripts SQL em vez de ficheiros .mdf/.ldf?

**Vantagens:**
- ✅ Ficheiros de texto (podem ser visualizados e comparados no GitHub)
- ✅ Compatíveis entre diferentes versões do SQL Server
- ✅ Ocupam menos espaço
- ✅ Fácil de executar para recriar a base de dados
- ✅ Permite ver mudanças no histórico do Git

**Problemas com .mdf/.ldf:**
- ❌ Ficheiros binários grandes (8+ MB)
- ❌ Incompatibilidades de versão
- ❌ Não podem ser visualizados no GitHub
- ❌ Difícil de identificar mudanças

## Passo a Passo: Exportar a Base de Dados

### 1. Abrir o SQL Server Management Studio (SSMS)

### 2. Conectar à sua instância do SQL Server

### 3. No Object Explorer, localizar a base de dados

Clique com o botão direito na base de dados **NEOs**

### 4. Gerar Scripts

Selecione: **Tasks** → **Generate Scripts...**

### 5. Wizard - Introduction

Clique em **Next**

### 6. Choose Objects

Selecione: **Script entire database and all database objects**

Clique em **Next**

### 7. Set Scripting Options

#### Opções Principais:
- ✅ Selecione: **Save as script file**
- ✅ Selecione: **Single script file**
- 📁 **File name**: Clique em `...` e navegue para a pasta do projeto Git
  - Exemplo: `C:\...\NEOs_Monitoring_System\NEOs_database.sql`
- ✅ Marque: **Overwrite existing file** (se necessário)

#### Configurações Avançadas (IMPORTANTE!):

Clique no botão **Advanced** e configure:

**General:**
- **Script USE DATABASE**: `False`
- **Types of data to script**: `Schema and data` ⚠️ **CRUCIAL!**

**Table/View Options:**
- **Script Check Constraints**: `True`
- **Script Foreign Keys**: `True`
- **Script Indexes**: `True`
- **Script Primary Keys**: `True`
- **Script Triggers**: `True`
- **Script Unique Keys**: `True`

Clique em **OK**

### 8. Summary

Reveja as opções e clique em **Next**

### 9. Save Scripts

Aguarde enquanto o script é gerado. Clique em **Finish** quando concluído.

## Adicionar ao GitHub

Depois de gerar o ficheiro `.sql`:

```powershell
cd NEOs_Monitoring_System
git add NEOs_database.sql
git commit -m "Add database schema and data"
git push
```

## Como Importar a Base de Dados (Para Colegas)

### Opção 1: Criar Nova Base de Dados

1. Abra o SQL Server Management Studio
2. Clique com o botão direito em **Databases** → **New Database...**
3. Nome: `NEOs` (ou outro nome)
4. Clique em **OK**
5. Abra o ficheiro `NEOs_database.sql` no SSMS (**File** → **Open** → **File**)
6. Na barra de ferramentas, selecione a base de dados `NEOs`
7. Clique em **Execute** (F5)
8. ✅ Base de dados completa criada!

### Opção 2: Usar Base de Dados Existente

Se já tiver uma base de dados criada:

1. Abra o ficheiro `NEOs_database.sql` no SSMS
2. Selecione a base de dados de destino
3. Execute o script (F5)

## Verificação

Após importar, verifique se tudo foi criado:

```sql
-- Ver todas as tabelas
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE'

-- Ver todas as views
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.VIEWS

-- Ver todos os triggers
SELECT name 
FROM sys.triggers

-- Contar registos em tabelas (exemplo)
SELECT 'NEO' as Tabela, COUNT(*) as Total FROM NEO
UNION ALL
SELECT 'Observatory', COUNT(*) FROM Observatory
-- ... adicione outras tabelas conforme necessário
```

## Notas Importantes

- ⚠️ O ficheiro `.sql` contém **toda a estrutura E todos os dados**
- 📝 Sempre que fizer mudanças significativas na BD, regenere o script
- 🔄 Mantenha o ficheiro `.sql` atualizado no GitHub
- 🚫 **NÃO** adicione ficheiros `.mdf` ou `.ldf` ao Git (adicione ao `.gitignore` se necessário)

## Troubleshooting

### Erro: "Database already exists"
- Apague a base de dados existente ou mude o nome

### Erro: "Foreign Key constraints"
- O script deve criar as tabelas na ordem correta. Se der erro, verifique se executou o script completo

### Dados não foram exportados
- Confirme que em **Advanced** selecionou: **Types of data to script** = `Schema and data`

---

**Última atualização:** Dezembro 2025
