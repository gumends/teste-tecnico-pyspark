# Descrição do Projeto
Este projeto realiza uma análise de dados de um e-commerce utilizando PySpark. Os dados de clientes e pedidos estão em formato JSON, e o objetivo é compreender o comportamento de compra dos clientes por meio de diversas análises.

# Tecnologias Utilizadas
* Python: Versão 3.12.3
* PySpark: Versão 4.1.0
* Java: Versão 21.0.9

# Componentes Principais
* Dados de Clientes: Aproximadamente 10 mil registros, contendo ID e nome do cliente.
* Dados de Pedidos: Cerca de 1 milhão de registros, com ID do pedido, ID do cliente e valor do pedido.

# Análises Realizadas
* Qualidade dos Dados: Verificação de possíveis falhas nos registros.
* Agregação de Dados: Cálculo da quantidade e do valor total de pedidos por cliente.
* Métricas Estatísticas: Cálculo de média, mediana e percentis dos valores de pedidos.
* Filtragem de Clientes: Identificação de clientes com valor total está entre o percentil 10 e 90.

# Como Executar
1. Clone o repositório.
2. Garanta que o ambiente tenha Python, Java e PySpark instalados.
3. Execute os scripts Python desejados.
