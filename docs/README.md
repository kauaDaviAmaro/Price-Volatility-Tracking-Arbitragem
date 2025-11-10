# Documentação de Demonstração

## Como Criar o GIF do Scraper

### Opção 1: Usando LICEcap (Windows/Mac)

1. Baixe e instale [LICEcap](https://www.cockos.com/licecap/)
2. Execute o scraper:
   ```bash
   python -m src.scraper_app
   ```
3. Abra o LICEcap e posicione a janela sobre o terminal
4. Clique em "Record" e ajuste o tamanho da captura
5. Execute o scraper e deixe rodar por alguns segundos
6. Pare a gravação e salve como `scraper-demo.gif` neste diretório

### Opção 2: Usando asciinema + agg (Linux/Mac)

1. Instale asciinema e agg:
   ```bash
   pip install asciinema
   # Para agg, siga: https://github.com/asciinema/agg
   ```
2. Grave a sessão:
   ```bash
   asciinema rec scraper-demo.cast
   # Execute: python -m src.scraper_app
   # Pressione Ctrl+D para parar
   ```
3. Converta para GIF:
   ```bash
   agg scraper-demo.cast scraper-demo.gif
   ```

### Opção 3: Usando OBS Studio (Qualquer OS)

1. Instale [OBS Studio](https://obsproject.com/)
2. Configure uma fonte de "Window Capture" apontando para o terminal
3. Inicie a gravação
4. Execute o scraper
5. Pare a gravação
6. Exporte como GIF usando um conversor ou extensão

### Conteúdo Recomendado para o GIF

O GIF deve mostrar:
- O scraper iniciando
- Logs de progresso
- Extração de dados
- Mensagens de sucesso
- Estatísticas finais

Duração recomendada: 10-30 segundos

