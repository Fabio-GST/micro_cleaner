const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const fs = require('fs');
const os = require('os');
const readline = require('readline');

const db = require('./db'); // Importar o módulo de banco de dados

if (isMainThread) {
  // Thread principal
  async function getFileInfo(filePath) {
    try {
      const stats = fs.statSync(filePath);
      const fileSizeInBytes = stats.size;
      const fileSizeInMB = fileSizeInBytes / (1024 * 1024);
      const fileSizeInGB = fileSizeInMB / 1024;

      console.log('\n=== INFORMAÇÕES DO ARQUIVO ===');
      console.log('Arquivo:', filePath);
      console.log('Tamanho:', fileSizeInBytes.toLocaleString(), 'bytes');
      console.log('Tamanho:', fileSizeInMB.toFixed(2), 'MB');
      console.log('Tamanho:', fileSizeInGB.toFixed(2), 'GB');
      console.log('===============================\n');

      return stats;
    } catch (error) {
      console.error('Erro ao ler informações do arquivo:', error.message);
      return null;
    }
  }

  async function getCSVHeader(filePath) {
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });

    for await (const line of rl) {
      if (line.trim()) {
        const header = line.split(';');
        rl.close();
        fileStream.destroy();
        console.log('Primeira linha (amostra):', header);
        console.log('Formato: [data_hora, numero/duração, numero]\n');
        return header;
      }
    }
    return null;
  }

  async function processFileInChunks(filePath, responseCode) {
    if (!fs.existsSync(filePath)) {
      console.log('\nERRO: Arquivo não encontrado...\n');
      return;
    }

    // Mostrar informações do arquivo
    await getFileInfo(filePath);
    await getCSVHeader(filePath);

    const startTime = Date.now();
    const chunkSize = 5000000; // 5 milhões de linhas por chunk
    const batchSize = 50000; // 50k linhas por batch
    const numCPUs = Math.min(os.cpus().length, 6); // Máximo 6 workers
    const maxConcurrentBatches = 3; // Máximo 3 batches simultâneos

    console.log(`Processando arquivo em chunks de ${chunkSize.toLocaleString()} linhas`);
    console.log(`Usando ${numCPUs} workers com batches de ${batchSize.toLocaleString()} linhas`);
    console.log(`Máximo ${maxConcurrentBatches} batches simultâneos\n`);

    let totalLines = 0;
    let chunkNumber = 0;
    const finalConsolidated = {
      calls200: {},
      calls404: {},
      calls487: {},
      total: 0
    };

    // Pool de workers pré-criados
    const workers = [];
    for (let i = 0; i < numCPUs; i++) {
      workers.push({
        worker: new Worker(__filename, { workerData: { isWorker: true } }),
        busy: false,
        id: i
      });
    }

    // Função para processar um chunk do arquivo
    const processChunk = async () => {
      return new Promise(async (resolve, reject) => {
        try {
          const fileStream = fs.createReadStream(filePath);
          const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity
          });

          let lineCount = 0;
          let skipLines = chunkNumber * chunkSize;
          let batch = [];
          let batchNumber = 0;
          const chunkConsolidated = {
            calls200: {},
            calls404: {},
            calls487: {},
            total: 0
          };

          // Array para controlar batches em processamento
          const activeBatches = [];

          // Função para processar batch
          const processBatch = async (batchData, batchNum) => {
            return new Promise((resolve, reject) => {
              let availableWorker = null;
              let attempts = 0;
              
              const findWorker = () => {
                availableWorker = workers.find(w => !w.busy);
                if (!availableWorker && attempts < 50) {
                  attempts++;
                  setTimeout(findWorker, 100);
                  return;
                }
                
                if (!availableWorker) {
                  reject(new Error(`Nenhum worker disponível após ${attempts * 100}ms`));
                  return;
                }

                availableWorker.busy = true;

                const messageHandler = (result) => {
                  consolidateResults(result, chunkConsolidated);
                  availableWorker.busy = false;
                  availableWorker.worker.off('message', messageHandler);
                  availableWorker.worker.off('error', errorHandler);
                  resolve(result);
                };

                const errorHandler = (error) => {
                  console.error(`Erro no batch ${batchNum}:`, error);
                  availableWorker.busy = false;
                  availableWorker.worker.off('message', messageHandler);
                  availableWorker.worker.off('error', errorHandler);
                  reject(error);
                };

                availableWorker.worker.once('message', messageHandler);
                availableWorker.worker.once('error', errorHandler);

                availableWorker.worker.postMessage({
                  lines: batchData,
                  batchNumber: batchNum,
                  responseCode: responseCode
                });
              };

              findWorker();
            });
          };

          // Processar linhas do chunk
          for await (const line of rl) {
            if (line.trim()) {
              totalLines++;
              lineCount++;

              // Pular linhas até chegar no chunk atual
              if (totalLines <= skipLines) {
                continue;
              }

              batch.push(line);

              // Processar batch quando estiver cheio
              if (batch.length === batchSize) {
                batchNumber++;
                const batchToProcess = [...batch];
                batch = [];

                // Aguardar se há muitos batches em processamento
                while (activeBatches.length >= maxConcurrentBatches) {
                  await Promise.race(activeBatches);
                  
                  // Remover batches concluídos
                  for (let i = activeBatches.length - 1; i >= 0; i--) {
                    try {
                      const result = await Promise.race([
                        activeBatches[i], 
                        new Promise(resolve => setTimeout(() => resolve('pending'), 10))
                      ]);
                      if (result !== 'pending') {
                        activeBatches.splice(i, 1);
                      }
                    } catch (error) {
                      activeBatches.splice(i, 1);
                    }
                  }
                }

                const batchPromise = processBatch(batchToProcess, batchNumber);
                activeBatches.push(batchPromise);

                // Log de progresso do chunk
                if (lineCount % 100000 === 0) {
                  const elapsed = (Date.now() - startTime) / 1000;
                  const linesPerSecond = totalLines / elapsed;
                  const memUsage = process.memoryUsage();
                  
                  console.log(`Chunk ${chunkNumber + 1}: ${lineCount.toLocaleString()} linhas processadas`);
                  console.log(`Total geral: ${totalLines.toLocaleString()} linhas (${Math.round(linesPerSecond).toLocaleString()} linhas/s)`);
                  console.log(`Memória: ${Math.round(memUsage.heapUsed / 1024 / 1024)}MB\n`);
                }
              }

              // Se atingiu o limite do chunk, parar
              if (lineCount >= chunkSize) {
                break;
              }
            }
          }

          // Processar último batch do chunk se houver
          if (batch.length > 0) {
            batchNumber++;
            const batchPromise = processBatch(batch, batchNumber);
            activeBatches.push(batchPromise);
          }

          // Aguardar todos os batches do chunk
          await Promise.allSettled(activeBatches);

          rl.close();
          fileStream.destroy();

          console.log(`\n✅ Chunk ${chunkNumber + 1} concluído: ${lineCount.toLocaleString()} linhas processadas`);
          console.log(`Dados consolidados do chunk:`);
          console.log(`  - Calls200: ${Object.keys(chunkConsolidated.calls200).length.toLocaleString()}`);
          console.log(`  - Calls404: ${Object.keys(chunkConsolidated.calls404).length.toLocaleString()}`);
          console.log(`  - Calls487: ${Object.keys(chunkConsolidated.calls487).length.toLocaleString()}\n`);

          // Consolidar dados do chunk no resultado final
          consolidateResults(chunkConsolidated, finalConsolidated);

          // Força garbage collection
          if (global.gc) {
            global.gc();
          }

          resolve({ processed: lineCount, hasMore: lineCount === chunkSize });

        } catch (error) {
          reject(error);
        }
      });
    };

    // Processar arquivo chunk por chunk
    let hasMore = true;
    while (hasMore) {
      chunkNumber++;
      console.log(`\n🔄 Iniciando processamento do Chunk ${chunkNumber}...`);
      
      try {
        const result = await processChunk();
        hasMore = result.hasMore;
      } catch (error) {
        console.error(`Erro no chunk ${chunkNumber}:`, error.message);
        break;
      }

      // Salvar dados no banco a cada chunk (opcional)
      if (chunkNumber % 2 === 0) { // A cada 2 chunks
        console.log(`\n💾 Salvando dados intermediários no banco...`);
        await saveToDB(finalConsolidated, responseCode);
        
        // Limpar dados salvos para economizar memória
        finalConsolidated.calls200 = {};
        finalConsolidated.calls404 = {};
        finalConsolidated.calls487 = {};
      }
    }

    // Fechar workers
    workers.forEach(w => {
      try {
        w.worker.terminate();
      } catch (error) {
        console.warn('Erro ao fechar worker:', error.message);
      }
    });

    // Salvar dados finais
    if (Object.keys(finalConsolidated.calls200).length > 0 || 
        Object.keys(finalConsolidated.calls404).length > 0 || 
        Object.keys(finalConsolidated.calls487).length > 0) {
      console.log(`\n💾 Salvando dados finais no banco...`);
      await saveToDB(finalConsolidated, responseCode);
    }

    const endTime = Date.now();
    const totalTime = (endTime - startTime) / 1000;
    const linesPerSecond = totalLines / totalTime;
    
    console.log(`\n=== RESUMO FINAL ===`);
    console.log(`Tempo total: ${totalTime.toFixed(1)}s (${(totalTime / 60).toFixed(1)} min)`);
    console.log(`Velocidade média: ${Math.round(linesPerSecond).toLocaleString()} linhas/s`);
    console.log(`Total processado: ${totalLines.toLocaleString()} linhas`);
    console.log(`Chunks processados: ${chunkNumber}`);
    console.log(`====================`);
  }

  function consolidateResults(result, consolidated) {
    // Consolidar calls200
    for (const number in result.calls200) {
      if (!consolidated.calls200[number] ||
        result.calls200[number].duration > consolidated.calls200[number].duration) {
        consolidated.calls200[number] = result.calls200[number];
      }
    }

    // Consolidar calls404
    for (const number in result.calls404) {
      consolidated.calls404[number] = result.calls404[number];
    }

    // Consolidar calls487
    for (const number in result.calls487) {
      if (!consolidated.calls487[number]) {
        consolidated.calls487[number] = result.calls487[number];
      } else {
        consolidated.calls487[number].attemps += result.calls487[number].attemps;
        consolidated.calls487[number].created_at = result.calls487[number].created_at;
        consolidated.calls487[number].updated_at = result.calls487[number].updated_at;
      }
    }

    consolidated.total += result.processed;
  }

  async function saveToDB(data, responseCode) {
    if (Object.keys(data.calls200).length === 0 && 
        Object.keys(data.calls404).length === 0 && 
        Object.keys(data.calls487).length === 0) {
      return;
    }

    console.log('Salvando dados no banco de dados...');

    const database = new db();
    await database.connect();

    switch(responseCode) {
      case 200:
        if (Object.keys(data.calls200).length > 0) {
          console.log(`Inserindo ${Object.keys(data.calls200).length.toLocaleString()} registros de calls200...`);
          await database.insertCalls200(data.calls200);
        }
        break;
      case 404:
        if (Object.keys(data.calls404).length > 0) {
          console.log(`Inserindo ${Object.keys(data.calls404).length.toLocaleString()} registros de calls404...`);
          await database.insertCalls404(data.calls404);
        }
        break;
      case 487:
        if (Object.keys(data.calls487).length > 0) {
          console.log(`Inserindo ${Object.keys(data.calls487).length.toLocaleString()} registros de calls487...`);
          await database.insertCalls487(data.calls487);
        }
        break;
    }

    await database.disconnect();
    console.log('✅ Dados salvos com sucesso!');
  }

  // Executar
  const filePath = process.argv[2];
  const responseCode = parseInt(process.argv[3]);

  if (!filePath || !responseCode) {
    console.log('Uso: node main.js <caminho_do_arquivo.csv> <codigo_resposta>');
    console.log('Exemplo: node main.js 200.csv 200');
    console.log('Exemplo: node main.js 404.csv 404');
    console.log('Exemplo: node main.js 487.csv 487');
    process.exit(1);
  }

  if (![200, 404, 487].includes(responseCode)) {
    console.log('Código de resposta deve ser: 200, 404 ou 487');
    process.exit(1);
  }

  processFileInChunks(filePath, responseCode);

} else {
  // Worker thread (otimizado)
  parentPort.on('message', ({ lines, batchNumber, responseCode }) => {
    const result = processLines(lines, responseCode);
    parentPort.postMessage(result);
  });

  function processLines(lines, responseCode) {
    const calls200 = {};
    const calls404 = {};
    const calls487 = {};
    let processed = 0;

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const data = line.split(';');

      if (data.length < 3) continue;

      const dateTime = data[0];
      const number = data[2];
      processed++;

      const createdAt = convertToISO(dateTime);
      const updatedAt = new Date().toISOString();

      switch (responseCode) {
        case 200:
          const duration = parseInt(data[1]) || 0;
          if (!calls200[number] || duration > calls200[number].duration) {
            calls200[number] = {
              created_at: createdAt,
              updated_at: updatedAt,
              number: number,
              duration: duration
            };
          }
          break;

        case 404:
          if (!calls404[number]) {
            calls404[number] = {
              created_at: createdAt,
              updated_at: updatedAt,
              number: number
            };
          }
          break;

        case 487:
          if (!calls487[number]) {
            calls487[number] = {
              created_at: createdAt,
              updated_at: updatedAt,
              number: number,
              attemps: 1
            };
          } else {
            calls487[number].attemps++;
            calls487[number].created_at = createdAt;
            calls487[number].updated_at = updatedAt;
          }
          break;
      }
    }

    return { calls200, calls404, calls487, processed };
  }

  function convertToISO(dateTimeString) {
    try {
      const date = new Date(dateTimeString);
      if (isNaN(date.getTime())) {
        return new Date().toISOString();
      }
      return date.toISOString();
    } catch (error) {
      return new Date().toISOString();
    }
  }
}