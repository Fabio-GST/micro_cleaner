const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const fs = require('fs');
const os = require('os');
const readline = require('readline');

const db = require('./db'); // Importar o módulo de banco de dados

if (isMainThread) {
  // Thread principal
  async function processCSVStreaming(filePath) {
    if (!fs.existsSync(filePath)) {
      console.log('\nERRO: Arquivo não encontrado...\n');
      return;
    }

    const startTime = Date.now();
    const batchSize = 10000; // Linhas por batch
    const numCPUs = os.cpus().length;

    console.log(`\nIniciando processamento em streaming com ${numCPUs} threads...\n`);

    // Configurar stream de leitura
    const fileStream = fs.createReadStream(filePath);
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

    // Pool de workers
    const workers = [];
    for (let i = 0; i < numCPUs; i++) {
      workers.push({
        worker: null,
        busy: false,
        id: i
      });
    }

    // Função para processar batch
    const processBatch = async (batchData, batchNum) => {
      return new Promise((resolve, reject) => {
        // Encontrar worker disponível
        const availableWorker = workers.find(w => !w.busy);

        if (!availableWorker) {
          // Se não há worker disponível, aguardar
          setTimeout(() => processBatch(batchData, batchNum).then(resolve).catch(reject), 100);
          return;
        }

        // Criar novo worker se necessário
        if (!availableWorker.worker) {
          availableWorker.worker = new Worker(__filename, {
            workerData: { isWorker: true }
          });
        }

        availableWorker.busy = true;

        // Configurar listeners
        const messageHandler = (result) => {
          // Consolidar resultados
          consolidateResults(result, consolidated);

          console.log(`Batch ${batchNum} processado: ${result.processed} linhas`);

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

        availableWorker.worker.on('message', messageHandler);
        availableWorker.worker.on('error', errorHandler);

        // Enviar dados para processamento
        availableWorker.worker.postMessage({
          lines: batchData,
          batchNumber: batchNum
        });
      });
    };

    // Array para controlar batches em processamento
    const activeBatches = [];

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

          // Processar batch assincronamente
          const batchPromise = processBatch(batchToProcess, batchNumber);
          activeBatches.push(batchPromise);

          // Limitar número de batches simultâneos
          if (activeBatches.length >= numCPUs * 2) {
            await Promise.race(activeBatches);
            // Remove batches concluídos
            for (let i = activeBatches.length - 1; i >= 0; i--) {
              if (await Promise.race([activeBatches[i], Promise.resolve('pending')]) !== 'pending') {
                activeBatches.splice(i, 1);
              }
            }
          }

          console.log(`Linhas processadas: ${lineCount}`);
        }
      }
    }

    // Processar último batch se houver
    if (batch.length > 0) {
      batchNumber++;
      await processBatch(batch, batchNumber);
    }

    // Aguardar todos os batches pendentes
    await Promise.all(activeBatches);

    // Fechar workers
    workers.forEach(w => {
      if (w.worker) {
        w.worker.terminate();
      }
    });

    // Salvar consolidado no banco
    console.log(`\nProcessamento de ${lineCount} linhas concluído. Salvando no banco...`);
    await saveToDB(consolidated);

    const endTime = Date.now();
    console.log(`\nProcessamento concluído em ${(endTime - startTime) / 1000}s`);
    console.log(`Total processado: ${consolidated.total} registros`);
  }

  function consolidateResults(result, consolidated) {
    // Consolidar calls200
    Object.keys(result.calls200).forEach(number => {
      if (!consolidated.calls200[number] ||
        result.calls200[number].duration > consolidated.calls200[number].duration) {
        consolidated.calls200[number] = result.calls200[number];
      }
    });

    // Consolidar calls404
    Object.keys(result.calls404).forEach(number => {
      consolidated.calls404[number] = result.calls404[number];
    });

    // Consolidar calls487
    Object.keys(result.calls487).forEach(number => {
      if (!consolidated.calls487[number]) {
        consolidated.calls487[number] = result.calls487[number];
      } else {
        consolidated.calls487[number].attemps += result.calls487[number].attemps;
      }
    });

    consolidated.total += result.processed;
  }

  async function saveToDB(data) {
    console.log('\nSalvando dados no banco de dados...\n');

    const database = new db();
    await database.connect();

    console.log(`Inserindo ${Object.keys(data.calls200).length} registros de calls200...`);
    console.log(`Inserindo ${Object.keys(data.calls404).length} registros de calls404...`);
    console.log(`Inserindo ${Object.keys(data.calls487).length} registros de calls487...`);

    await database.insertCalls200(data.calls200);
    await database.insertCalls404(data.calls404);
    await database.insertCalls487(data.calls487);

    await database.disconnect();

    console.log('Dados salvos com sucesso!');
  }

  // Executar
  const filePath = process.argv[2];
  if (!filePath) {
    console.log('Uso: node main.js <caminho_do_arquivo.csv>');
    process.exit(1);
  }

  processCSVStreaming(filePath);

} else {
  // Worker thread
  parentPort.on('message', ({ lines, batchNumber }) => {
    const result = processLines(lines);
    parentPort.postMessage(result);
  });

  function processLines(lines) {
    const calls200 = {};
    const calls404 = {};
    const calls487 = {};
    let processed = 0;

    lines.forEach(line => {
      const data = line.split(',');

      if (data.length < 3) {
        return;
      }

      const number = data[0];
      const responseCode = parseInt(data[2]);

      processed++;

      switch (responseCode) {
        case 200:
          if (data.length >= 4) {
            const duration = parseInt(data[3]) || 0;
            if (!calls200[number] || duration > calls200[number].duration) {
              calls200[number] = {
                created_at: new Date().toISOString(),
                number: number,
                duration: duration
              };
            }
          }
          break;

        case 404:
          if (!calls404[number]) {
            calls404[number] = {
              created_at: new Date().toISOString(),
              number: number
            };
          }
          break;

        case 487:
          if (!calls487[number]) {
            calls487[number] = {
              created_at: new Date().toISOString(),
              number: number,
              attemps: 1
            };
          } else {
            calls487[number].attemps++;
          }
          break;
      }
    });

    return { calls200, calls404, calls487, processed };
  }
}