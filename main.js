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
        console.log('Formato: [data_hora, numero, duração/codigo]\n');
        return header;
      }
    }
    return null;
  }

  async function processCSVStreaming(filePath, responseCode) {
    if (!fs.existsSync(filePath)) {
      console.log('\nERRO: Arquivo não encontrado...\n');
      return;
    }

    // Mostrar informações do arquivo
    await getFileInfo(filePath);
    await getCSVHeader(filePath);

    const startTime = Date.now();
    const batchSize = 25000; // Aumentado para 50k linhas por batch
    const numCPUs = Math.min(os.cpus().length, 8); // Limitar a 8 workers
    const maxConcurrentBatches = 4; // Limitar batches simultâneos

    console.log(`Processando arquivo como código ${responseCode}`);
    console.log(`Usando ${numCPUs} workers com batches de ${batchSize.toLocaleString()} linhas`);
    console.log(`Máximo ${maxConcurrentBatches} batches simultâneos\n`);

    // Configurar stream de leitura com buffer maior
    const fileStream = fs.createReadStream(filePath, {
      highWaterMark: 1024 * 1024 // Buffer de 1MB
    });
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });

    let lineCount = 0;
    let batch = [];
    let batchNumber = 0;
    const consolidated = {
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

    // Array para controlar batches em processamento
    const activeBatches = [];

    // Função otimizada para processar batch
    const processBatch = async (batchData, batchNum) => {
      return new Promise((resolve, reject) => {
        // Encontrar worker disponível
        let availableWorker = null;
        let attempts = 0;
        
        const findWorker = () => {
          availableWorker = workers.find(w => !w.busy);
          if (!availableWorker && attempts < 50) { // Máximo 5 segundos esperando
            attempts++;
            setTimeout(findWorker, 100);
            return;
          }
          
          if (!availableWorker) {
            reject(new Error(`Nenhum worker disponível após ${attempts * 100}ms`));
            return;
          }

          availableWorker.busy = true;

          // Configurar listeners únicos
          const messageHandler = (result) => {
            consolidateResults(result, consolidated);
            
            if (batchNum % 10 === 0) { // Log a cada 10 batches
              console.log(`Batch ${batchNum} processado: ${result.processed} linhas`);
            }

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

          // Enviar dados para processamento
          availableWorker.worker.postMessage({
            lines: batchData,
            batchNumber: batchNum,
            responseCode: responseCode
          });
        };

        findWorker();
      });
    };

    // Processar arquivo linha por linha
    for await (const line of rl) {
      if (line.trim()) {
        batch.push(line);
        lineCount++;

        // Quando batch estiver cheio, processar
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

          // Processar batch assincronamente
          const batchPromise = processBatch(batchToProcess, batchNumber);
          activeBatches.push(batchPromise);

          if (lineCount % 100000 === 0) {
            const elapsed = (Date.now() - startTime) / 1000;
            const linesPerSecond = lineCount / elapsed;
            console.log(`Progresso: ${lineCount.toLocaleString()} linhas em ${elapsed.toFixed(1)}s (${Math.round(linesPerSecond).toLocaleString()} linhas/s)`);
            
            // Força garbage collection se disponível
            if (global.gc) {
              global.gc();
            }
          }
        }
      }
    }

    // Processar último batch se houver
    if (batch.length > 0) {
      batchNumber++;
      const batchPromise = processBatch(batch, batchNumber);
      activeBatches.push(batchPromise);
    }

    // Aguardar todos os batches pendentes
    console.log('\nAguardando conclusão de todos os batches...');
    await Promise.all(activeBatches);

    // Fechar workers
    workers.forEach(w => {
      w.worker.terminate();
    });

    // Salvar consolidado no banco
    console.log(`\nProcessamento de ${lineCount.toLocaleString()} linhas concluído. Salvando no banco...`);
    await saveToDB(consolidated, responseCode);

    const endTime = Date.now();
    const totalTime = (endTime - startTime) / 1000;
    const linesPerSecond = lineCount / totalTime;
    
    console.log(`\nProcessamento concluído em ${totalTime.toFixed(1)}s`);
    console.log(`Velocidade média: ${Math.round(linesPerSecond).toLocaleString()} linhas/s`);
    console.log(`Total processado: ${consolidated.total} registros`);
  }

  function consolidateResults(result, consolidated) {
    // Consolidar calls200 (otimizado)
    for (const number in result.calls200) {
      if (!consolidated.calls200[number] ||
        result.calls200[number].duration > consolidated.calls200[number].duration) {
        consolidated.calls200[number] = result.calls200[number];
      }
    }

    // Consolidar calls404 (otimizado)
    for (const number in result.calls404) {
      consolidated.calls404[number] = result.calls404[number];
    }

    // Consolidar calls487 (otimizado)
    for (const number in result.calls487) {
      if (!consolidated.calls487[number]) {
        consolidated.calls487[number] = result.calls487[number];
      } else {
        consolidated.calls487[number].attemps += result.calls487[number].attemps;
        consolidated.calls487[number].created_at = result.calls487[number].created_at;
      }
    }

    consolidated.total += result.processed;
  }

  async function saveToDB(data, responseCode) {
    console.log('\nSalvando dados no banco de dados...\n');

    const database = new db();
    await database.connect();

    switch(responseCode) {
      case 200:
        console.log(`Inserindo ${Object.keys(data.calls200).length} registros de calls200...`);
        await database.insertCalls200(data.calls200);
        break;
      case 404:
        console.log(`Inserindo ${Object.keys(data.calls404).length} registros de calls404...`);
        await database.insertCalls404(data.calls404);
        break;
      case 487:
        console.log(`Inserindo ${Object.keys(data.calls487).length} registros de calls487...`);
        await database.insertCalls487(data.calls487);
        break;
      default:
        console.log('Código de resposta não reconhecido:', responseCode);
    }

    await database.disconnect();
    console.log('Dados salvos com sucesso!');
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

  processCSVStreaming(filePath, responseCode);

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

      if (data.length < 2) continue;

      const dateTime = data[0];
      const number = data[2];
      processed++;

      // Converter data_hora para formato ISO
      const createdAt = convertToISO(dateTime);

      switch (responseCode) {
        case 200:
          if (data.length >= 3) {
            const duration = parseInt(data[2]) || 0;
            if (!calls200[number] || duration > calls200[number].duration) {
              calls200[number] = {
                created_at: createdAt,
                number: number,
                duration: duration
              };
            }
          }
          break;

        case 404:
          if (!calls404[number]) {
            calls404[number] = {
              created_at: createdAt,
              number: number
            };
          }
          break;

        case 487:
          if (!calls487[number]) {
            calls487[number] = {
              created_at: createdAt,
              number: number,
              attemps: 1
            };
          } else {
            calls487[number].attemps++;
            calls487[number].created_at = createdAt;
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